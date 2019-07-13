package privval

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tendermint/tendermint/crypto/ed25519"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/privval"

	"github.com/tendermint/tendermint/types"
)

const (
	defaultTimeoutAcceptSeconds    = 3
	defaultTimeoutReadWriteSeconds = 3
)

var (
	testTimeoutAccept = defaultTimeoutAcceptSeconds * time.Second

	testTimeoutReadWrite    = 100 * time.Millisecond
	testTimeoutReadWrite2o3 = 66 * time.Millisecond // 2/3 of the other one

	testTimeoutHeartbeat    = 10 * time.Millisecond
	testTimeoutHeartbeat3o2 = 6 * time.Millisecond // 3/2 of the other one
)

type socketTestCase struct {
	addr   string
	dialer privval.SocketDialer
}

// testUnixAddr will attempt to obtain a platform-independent temporary file
// name for a Unix socket
func testUnixAddr() (string, error) {
	f, err := ioutil.TempFile("", "tendermint-privval-test-*")
	if err != nil {
		return "", err
	}
	addr := f.Name()
	f.Close()
	os.Remove(addr)
	return addr, nil
}

func socketTestCases(t *testing.T) []socketTestCase {
	tcpAddr := fmt.Sprintf("tcp://%s", testFreeTCPAddr(t))
	unixFilePath, err := testUnixAddr()
	require.NoError(t, err)
	unixAddr := fmt.Sprintf("unix://%s", unixFilePath)
	return []socketTestCase{
		{
			addr:   tcpAddr,
			dialer: privval.DialTCPFn(tcpAddr, testTimeoutReadWrite, ed25519.GenPrivKey()),
		},
		{
			addr:   unixAddr,
			dialer: privval.DialUnixFn(unixFilePath),
		},
	}
}

func TestSocketPVDeadline(t *testing.T) {
	for _, tc := range socketTestCases(t) {
		func() {
			var (
				listenc           = make(chan struct{})
				thisConnTimeout   = 100 * time.Millisecond
				validatorEndpoint = newSignerValidatorEndpoint(log.TestingLogger(), tc.addr, thisConnTimeout)
			)

			go func(sc *SignerValidatorEndpoint) {
				defer close(listenc)

				// Note: the TCP connection times out at the accept() phase,
				// whereas the Unix domain sockets connection times out while
				// attempting to fetch the remote signer's public key.
				assert.True(t, privval.IsConnTimeout(sc.Start()))

				assert.False(t, sc.IsRunning())
			}(validatorEndpoint)

			for {
				_, err := cmn.Connect(tc.addr)
				if err == nil {
					break
				}
			}

			<-listenc
		}()
	}
}
func TestRetryConnToRemoteSigner(t *testing.T) {
	for _, tc := range socketTestCases(t) {
		func() {
			var (
				logger  = log.TestingLogger()
				chainID = cmn.RandStr(12)
				readyCh = make(chan struct{})

				serviceEndpoint = privval.NewSignerServiceEndpoint(
					logger,
					chainID,
					types.NewMockPV(),
					tc.dialer,
				)
				thisConnTimeout   = testTimeoutReadWrite
				validatorEndpoint = newSignerValidatorEndpoint(logger, tc.addr, thisConnTimeout)
			)
			// Ping every:
			SignerValidatorEndpointSetHeartbeat(testTimeoutHeartbeat)(validatorEndpoint)

			privval.SignerServiceEndpointTimeoutReadWrite(testTimeoutReadWrite)(serviceEndpoint)
			privval.SignerServiceEndpointConnRetries(10)(serviceEndpoint)

			testStartEndpoint(t, readyCh, validatorEndpoint)
			defer validatorEndpoint.Stop()
			require.NoError(t, serviceEndpoint.Start())
			assert.True(t, serviceEndpoint.IsRunning())

			<-readyCh
			time.Sleep(testTimeoutHeartbeat * 2)

			serviceEndpoint.Stop()
			rs2 := privval.NewSignerServiceEndpoint(
				logger,
				chainID,
				types.NewMockPV(),
				tc.dialer,
			)
			// let some pings pass
			time.Sleep(testTimeoutHeartbeat3o2)
			require.NoError(t, rs2.Start())
			assert.True(t, rs2.IsRunning())
			defer rs2.Stop()

			// give the client some time to re-establish the conn to the remote signer
			// should see sth like this in the logs:
			//
			// E[10016-01-10|17:12:46.128] Ping                                         err="remote signer timed out"
			// I[10016-01-10|17:16:42.447] Re-created connection to remote signer       impl=SocketVal
			time.Sleep(testTimeoutReadWrite * 2)
		}()
	}
}

func newSignerValidatorEndpoint(logger log.Logger, addr string, timeoutReadWrite time.Duration) *SignerValidatorEndpoint {
	proto, address := cmn.ProtocolAndAddress(addr)

	ln, err := net.Listen(proto, address)
	logger.Info("Listening at", "proto", proto, "address", address)
	if err != nil {
		panic(err)
	}

	var listener net.Listener

	if proto == "unix" {
		unixLn := privval.NewUnixListener(ln)
		privval.UnixListenerTimeoutAccept(testTimeoutAccept)(unixLn)
		privval.UnixListenerTimeoutReadWrite(timeoutReadWrite)(unixLn)
		listener = unixLn
	} else {
		tcpLn := privval.NewTCPListener(ln, ed25519.GenPrivKey())
		privval.TCPListenerTimeoutAccept(testTimeoutAccept)(tcpLn)
		privval.TCPListenerTimeoutReadWrite(timeoutReadWrite)(tcpLn)
		listener = tcpLn
	}

	return NewSignerValidatorEndpoint(logger, listener)
}

func testSetupSocketPair(
	t *testing.T,
	chainID string,
	privValidator types.PrivValidator,
	addr string,
	socketDialer privval.SocketDialer,
) (*SignerValidatorEndpoint, *privval.SignerServiceEndpoint) {
	var (
		logger          = log.TestingLogger()
		privVal         = privValidator
		readyc          = make(chan struct{})
		serviceEndpoint = privval.NewSignerServiceEndpoint(
			logger,
			chainID,
			privVal,
			socketDialer,
		)

		thisConnTimeout   = testTimeoutReadWrite
		validatorEndpoint = newSignerValidatorEndpoint(logger, addr, thisConnTimeout)
	)

	SignerValidatorEndpointSetHeartbeat(testTimeoutHeartbeat)(validatorEndpoint)
	privval.SignerServiceEndpointTimeoutReadWrite(testTimeoutReadWrite)(serviceEndpoint)
	privval.SignerServiceEndpointConnRetries(1e6)(serviceEndpoint)

	testStartEndpoint(t, readyc, validatorEndpoint)

	require.NoError(t, serviceEndpoint.Start())
	assert.True(t, serviceEndpoint.IsRunning())

	<-readyc

	return validatorEndpoint, serviceEndpoint
}

func testReadWriteResponse(t *testing.T, resp RemoteSignerMsg, rsConn net.Conn) {
	_, err := readMsg(rsConn)
	require.NoError(t, err)

	err = writeMsg(rsConn, resp)
	require.NoError(t, err)
}

func testStartEndpoint(t *testing.T, readyCh chan struct{}, sc *SignerValidatorEndpoint) {
	go func(sc *SignerValidatorEndpoint) {
		require.NoError(t, sc.Start())
		assert.True(t, sc.IsRunning())

		readyCh <- struct{}{}
	}(sc)
}

// testFreeTCPAddr claims a free port so we don't block on listener being ready.
func testFreeTCPAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	return fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
}
