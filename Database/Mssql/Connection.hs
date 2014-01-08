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
    Sock.setSocketOption sock Sock.RecvTimeOut 1000
    Sock.setSocketOption sock Sock.SendTimeOut 1000
    s <- Sock.socketToHandle sock ReadWriteMode

    -- sending prelogin request
    let instbytes = B.snoc (E.encodeLazyByteString ASCII inst) 0
        prelogin = Map.fromList [(preloginVersion, B.pack [0, 0, 0, 0, 0, 0]),
                                 (preloginEncryption, B.pack [2]),
                                 (preloginInstOpt, instbytes),
                                 (preloginThreadId, B.pack [0, 0, 0, 0]),
                                 (preloginMars, B.pack [0])]
    sendPreLogin s prelogin
    -- reading prelogin response
    _ <- recvPreLogin s
    -- sending login request
    let login = (verTDS74, 4096, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                 "", username, password, "", "", B.empty, "", "", "",
                 (MacAddress 0 0 0 0 0 0), B.empty, "", "")
    sendLogin s login
    -- reading login response
    tokens <- recvTokens s
    let errors = filter isTokError tokens
    if (filter isTokLoginAck tokens) == []
            then fail (let srverr = SU.join " " [message e | e <- errors]
                       in if srverr == "" then "Login failed." else srverr)
            else return Impl.Connection {Impl.disconnect = fdisconnect s,
                                         Impl.runRaw = frunRaw s,
                                         Impl.prepare = newSth s}

fdisconnect :: Handle -> IO ()
fdisconnect = hClose

frunRaw :: Handle -> String -> IO ()
frunRaw s query = do
    tokens <- exec s query
    print tokens
    fail "errors"
