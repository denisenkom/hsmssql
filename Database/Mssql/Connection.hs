module Database.Mssql.Connection where
import qualified Database.Mssql.ConnectionImpl as Impl
import Database.Mssql.Statement
import Database.Mssql.Tds

import Data.String.Utils as SU
import qualified Data.ByteString.Lazy as B
import qualified Data.Encoding as E
import Data.Encoding.ASCII
import qualified Data.Map as Map
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
    sendPreLogin s prelogin bufSize
    -- reading prelogin response
    _ <- recvPreLogin s
    -- sending login request
    let login = (verTDS74, (fromIntegral bufSize), 0, 0, 0, 0, 0, 0, 0, 0, 0,
                 "", username, password, "", "", B.empty, "", "", "",
                 (MacAddress 0 0 0 0 0 0), B.empty, "", "")
    sendLogin s login bufSize
    -- reading login response
    tokens <- recvTokens s
    -- TODO: get bufSize from response
    let errors = filter isTokError tokens
    if (filter isTokLoginAck tokens) == []
            then fail (let srverr = SU.join " " [message e | e <- errors]
                       in if srverr == "" then "Login failed." else srverr)
            else return Impl.Connection {Impl.disconnect = fdisconnect s,
                                         Impl.runRaw = frunRaw s bufSize,
                                         Impl.prepare = newSth s bufSize}

fdisconnect :: Handle -> IO ()
fdisconnect = hClose

frunRaw :: Handle -> Int -> String -> IO ()
frunRaw s bufSize query = do
    tokens <- exec s query bufSize
    print tokens
    fail "errors"
