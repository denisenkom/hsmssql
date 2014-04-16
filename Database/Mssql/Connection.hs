module Database.Mssql.Connection where
import qualified Database.Mssql.ConnectionImpl as Impl
import Database.Mssql.Statement
import Database.Mssql.Tds

import Data.Bits
import Data.String.Utils as SU
import qualified Data.ByteString.Lazy as B
import qualified Data.Encoding as E
import Data.Encoding.ASCII
import qualified Data.Map as Map
import Database.HDBC
import qualified Network as Net
import qualified Network.Socket as Sock
import System.IO


connectMssql :: String -> String -> String -> String -> IO Impl.Connection
connectMssql host inst username password = do
    port <- getPort host inst
    let hint = Sock.defaultHints {Sock.addrFlags = [Sock.AI_NUMERICSERV],
                                  Sock.addrSocketType = Sock.Stream}
    addresses <- Sock.getAddrInfo (Just hint) (Just host) ((Just . show) port)
    let address = head addresses
    sock <- Sock.socket (Sock.addrFamily address) Sock.Stream (Sock.addrProtocol address)
    Sock.connect sock (Sock.addrAddress address)
    s <- Sock.socketToHandle sock ReadWriteMode

    -- sending prelogin request
    let bufSize = 4096 :: Int
        instbytes = B.snoc (E.encodeLazyByteString ASCII inst) 0
        prelogin = Map.fromList [(preloginVersion, B.pack [0, 0, 0, 0, 0, 0]),
                                 (preloginEncryption, B.pack [2]),
                                 (preloginInstOpt, instbytes),
                                 (preloginThreadId, B.pack [0, 0, 0, 0]),
                                 (preloginMars, B.pack [0])]
    sendPacketLazy bufSize s packPrelogin $ putPreLogin prelogin
    -- reading prelogin response
    _ <- recvPacketLazy s getPreLoginHead
    -- sending login request
    let login = (verTDS74, (fromIntegral bufSize), 0, 0, 0, 0, 0, 0, 0, 0, 0,
                 "", username, password, "", "", B.empty, "", "", "",
                 (MacAddress 0 0 0 0 0 0), B.empty, "", "")
    sendPacketLazy bufSize s packLogin7 $ putLogin login
    -- reading login response
    tokens <- recvPacketLazy s getTokens
    -- TODO: get bufSize from response
    let errors = filter isTokError tokens
    if (filter isTokLoginAck tokens) == []
            then fail (let srverr = SU.join " " [message e | e <- errors]
                       in if srverr == "" then "Login failed." else srverr)
            else return Impl.Connection {Impl.disconnect = fdisconnect s,
                                         Impl.commit = fcommit s bufSize,
                                         Impl.rollback = frollback s bufSize,
                                         Impl.runRaw = frunRaw s bufSize,
                                         Impl.prepare = newSth s bufSize}

fdisconnect :: Handle -> IO ()
fdisconnect = hClose

fcommit :: Handle -> Int -> IO ()
fcommit s bufSize = do
    let headers = [DataStmTransDescrHdr 0 1]
    sendPacketLazy bufSize s packTranMgr $ putCommitTran headers "" contTranFlag 0 ""
    tokens <- recvPacketLazy s getTokens
    return ()

frollback :: Handle -> Int -> IO ()
frollback s bufSize = do
    let headers = [DataStmTransDescrHdr 0 1]
    sendPacketLazy bufSize s packTranMgr $ putRollbackTran headers "" contTranFlag 0 ""
    tokens <- recvPacketLazy s getTokens
    return ()

frunRaw :: Handle -> Int -> String -> IO ()
frunRaw s bufSize query = do
    tokens <- exec s query bufSize
    let processResp ((TokError num state cls message srvname procname lineno):xs) messages =
            processResp xs ((num, state, cls, message, srvname, procname, lineno):messages)
        processResp ((TokInfo num state cls message srvname procname lineno):xs) messages =
            processResp xs ((num, state, cls, message, srvname, procname, lineno):messages)
        processResp (done@(TokDone status _ _):xs) messages =
            if (status .&. doneMoreResults == 0)
                then if xs == []
                    then (messages, isSuccess status)
                    else error "unexpected tokens after final DONE token"
                else processResp xs messages
        processResp (_:xs) messages = processResp xs messages

    let (messages, ok) = processResp tokens []
    if ok
        then return ()
        else do
            throwSqlError (if messages == []
                then SqlError {seState = "", seNativeError = -1, seErrorMsg = "Query failed, server didn't send an error"}
                else let (errno, _, _, msg, _, _, _) = head messages
                     in SqlError {seState = "", seNativeError = errno, seErrorMsg = msg})
