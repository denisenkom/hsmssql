module Main where

import qualified Network.Socket as Sock
import qualified Network as Net
import Data.Word
import Network.Socket.ByteString
import Data.Binary.Strict.Get
import Data.Binary.Put
import Data.Text.Encoding
--import qualified Data.Text as T
import qualified Data.Map as Map
import qualified Codec.Binary.UTF8.String as UTF8
import Data.List.Split(splitOn)
import Data.ByteString(unpack)
import qualified Data.ByteString.Lazy as B
import System.Environment
import Data.Maybe
import Control.Monad

foreign import ccall unsafe "htons" htons :: Word16 -> Word16

parseInstances :: Get ([String])
parseInstances = do
    pref <- getWord8
    _ <- getWord16be
    if pref == 5
        then do
            --x <- Data.Text.Encoding.decodeASCII (getBytes remaining)
            rem <- remaining
            str <- getByteString rem
            let tokens = (splitOn ";" (UTF8.decode (unpack str)))
            return tokens
        else return []

tokensToDict [] = Map.empty

--tokensToDictImpl :: Map.Map T.Text T.Text -> [T.Text] -> Map.Map T.Text T.Text

tokensToDictImpl :: [Map.Map String String] -> Map.Map String String -> [String] -> [Map.Map String String]
tokensToDictImpl l m [] = l
tokensToDictImpl l m ("":"":[]) = (m:l)
tokensToDictImpl l m ("":xs) = tokensToDictImpl (m:l) Map.empty xs
tokensToDictImpl l m (k:v:xs) = tokensToDictImpl l (Map.insert k v m) xs

fn (Right a, b) = tokensToDictImpl [] Map.empty a

isInst name m = case Map.lookup "InstanceName" m of Just n -> n == name
                                                    Nothing -> False


queryInstances :: String -> IO [Map.Map String String]
queryInstances hoststr = do
    s <- Sock.socket Sock.AF_INET Sock.Datagram 0
    host <- Sock.inet_addr hoststr
    let addr = Sock.SockAddrInet (Sock.PortNum (htons 1434)) host
    sent <- Sock.sendTo s "\x3" addr
    res <- Network.Socket.ByteString.recv s (16 * 1024 - 1)
    let tokens = runGet parseInstances res
    return $ fn tokens

packPrelogin = 18

serializePacket :: B.ByteString -> Put
serializePacket packet = do
    putWord8 packPrelogin
    putWord8 1  -- final packet
    putWord16be $ 8 + fromIntegral (B.length packet)
    putWord16be 0 -- spid
    putWord8 0 -- packet no
    putWord8 0 -- padding
    putLazyByteString packet
    flush

preparePreLoginImpl :: Int -> [(Word8, B.ByteString)] -> [(Word8, B.ByteString, Int)]
preparePreLoginImpl _ [] = []
preparePreLoginImpl offset [(k, v)] = [(k, v, offset)]
preparePreLoginImpl offset ((k, v):xs) = 
    let offset' = (offset + fromIntegral (B.length v))
    in (k, v, offset):(preparePreLoginImpl offset' xs)

preloginVersion = 0
preloginEncryption = 1
preloginInstOpt = 2
preloginThreadId = 3
preloginMars = 4
preloginTraceId = 5
preloginTerminator = 0xff

serializePreLogin :: Map.Map Word8  B.ByteString -> Put
serializePreLogin fields = do
    let fieldsWithOffsets = preparePreLoginImpl ((Map.size fields) * 5 + 1) (Map.assocs fields)
    forM fieldsWithOffsets (\(k, v, offset) -> do
        putWord8 k
        putWord16be $ fromIntegral offset
        putWord16be (fromIntegral (B.length v) :: Word16))
    putWord8 preloginTerminator
    forM (Map.elems fields) (\v -> do
        putLazyByteString v)
    return ()

main = do
    hoststr <- getEnv "HOST"
    instances <- queryInstances hoststr
    --print (tokensToDictImpl tokens)
    inst <- getEnv "INSTANCE"
    let ports = catMaybes [Map.lookup "tcp" i| i <- instances, isInst inst i]
    print ports
    let port = htons (read (head ports) :: Word16)
    print port
    s <- Net.connectTo hoststr (Net.PortNumber $ Sock.PortNum port)
    let instbytes = B.pack ((UTF8.encode inst) ++ [0])
    let prelogin = Map.fromList [(preloginVersion, B.pack [0, 0, 0, 0, 0, 0]),
                                 (preloginEncryption, B.pack [2]),
                                 (preloginInstOpt, instbytes),
                                 (preloginThreadId, B.pack [0, 0, 0, 0]),
                                 (preloginMars, B.pack [0])]
    print $ preparePreLoginImpl ((Map.size prelogin) * 5 + 1) (Map.assocs prelogin)
    let preloginbuf = runPut (serializePreLogin prelogin)
    print preloginbuf
    let preloginpacket = runPut (serializePacket preloginbuf)
    print preloginpacket
    B.hPutStr s preloginpacket
    resp <- B.hGet s 10 
    print resp
