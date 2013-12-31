--module Main where
module Database.Mssql.Tds where

import qualified Network.Socket as Sock
import qualified Network as Net
import Data.Bits
import Data.Sequence
import Data.Word
import Network.Socket.ByteString
import qualified Data.Binary.Get as LG
import Data.Binary.Strict.Get
import Data.Binary.Put
import qualified Data.Encoding as E
import Data.Encoding.UTF16
--import Data.Text.Encoding
--import qualified Data.Text as T
import qualified Data.Map as Map
import qualified Codec.Binary.UTF8.String as UTF8
import Data.List.Split(splitOn)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as B
import System.Environment
import System.IO
import Data.Maybe
import Control.Monad

foreign import ccall unsafe "htons" htons :: Word16 -> Word16

data MacAddress = MacAddress Word8 Word8 Word8 Word8 Word8 Word8

data Token = TokError {number :: Word32, state :: Word8, cls :: Word8, message :: String, srvname :: String, procname :: String, lineno :: Word32}
           | TokLoginAck {iface :: Word8, tdsver :: Word32, progname :: String, progver :: Word32}
           | TokEnvChange [EnvChange]
           | TokDone Word16 Word16 Word64
           | TokZero
     deriving(Show)

data EnvChange = PacketSize Int Int
     deriving(Show)

parseInstancesImpl :: Get ([String])
parseInstancesImpl = do
    pref <- getWord8
    _ <- getWord16be
    if pref == 5
        then do
            rem <- remaining
            str <- getByteString rem
            let tokens = splitOn ";" $ (UTF8.decode . BS.unpack) str
            return tokens
        else return []

parseInstances :: BS.ByteString -> Either String [Map.Map String String]
parseInstances s = 
    let res = runGet parseInstancesImpl s
    in case res of
        (Right tokens, _) -> Right $ tokensToDict tokens
        (Left err, _) -> Left err

tokensToDict tokens = tokensToDictImpl [] Map.empty tokens

tokensToDictImpl :: [Map.Map String String] -> Map.Map String String -> [String] -> [Map.Map String String]
tokensToDictImpl l m [] = l
tokensToDictImpl l m ("":"":[]) = (m:l)
tokensToDictImpl l m ("":xs) = tokensToDictImpl (m:l) Map.empty xs
tokensToDictImpl l m (k:v:xs) = tokensToDictImpl l (Map.insert k v m) xs

isInst name m = case Map.lookup "InstanceName" m of Just n -> n == name
                                                    Nothing -> False


queryInstances :: String -> IO [Map.Map String String]
queryInstances hoststr = do
    s <- Sock.socket Sock.AF_INET Sock.Datagram 0
    host <- Sock.inet_addr hoststr
    let addr = Sock.SockAddrInet (Sock.PortNum (htons 1434)) host
    sent <- Sock.sendTo s "\x3" addr
    res <- Network.Socket.ByteString.recv s (16 * 1024 - 1)
    let parse_res = parseInstances res
    case parse_res of
        Right instances -> return instances
        Left err -> do
            print err
            return []

packLogin7 = 16
packPrelogin = 18

serializePacket :: Word8 -> B.ByteString -> Put
serializePacket packettype packet = do
    putWord8 packettype
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

getPacket :: Handle -> IO (Word8, Bool, B.ByteString)
getPacket s = do
    let decode = do
            packtype <- LG.getWord8
            isfinal <- LG.getWord8
            size <- LG.getWord16be
            LG.getWord16be  -- spid
            LG.getWord8  -- packet no
            LG.getWord8  -- padding
            return (packtype, isfinal /= 0, size)

    hdrbuf <- B.hGet s 8
    print hdrbuf
    let (packtype, isfinal, size) = LG.runGet decode hdrbuf
    content <- B.hGet s ((fromIntegral size) - 8)
    return (packtype, isfinal, content)

getPreLoginHeadImpl :: Seq (Word8, Word16, Word16) -> LG.Get (Seq (Word8, Word16, Word16))
getPreLoginHeadImpl fields = do
    rectype <- LG.getWord8
    if rectype == preloginTerminator
        then return fields
        else do
            offset <- LG.getWord16be
            size <- LG.getWord16be
            getPreLoginHeadImpl $ fields |> (rectype, offset, size)

getPreLoginHead :: LG.Get (Seq (Word8, Word16, Word16))
getPreLoginHead = do
    getPreLoginHeadImpl empty

encodeUcs2 :: String -> B.ByteString
encodeUcs2 s = E.encodeLazyByteString UTF16LE s


manglePasswordByte :: Word8 -> Word8
manglePasswordByte ch = ((ch `shift` (-4)) .&. 0xff .|. (ch `shift` 4)) `xor` 0xA5

manglePassword :: String -> B.ByteString
manglePassword = (B.map manglePasswordByte) . encodeUcs2

verTDS74 = 0x74000004

serializeLogin :: (Word32, Word32, Word32, Word32, Word32,
                  Word8, Word8, Word8, Word8, Word32,
                  Word32, String, String, String, String,
                  String, B.ByteString, String, String, String,
                  MacAddress, B.ByteString, String,
                  String) -> Put
serializeLogin (tdsver, packsize, clientver, pid, connid, optflags1, optflags2,
               typeflags, optflags3, clienttz, clientlcid, hostname,
               username, password, appname, servername, extensionbuf, ctlintname,
               language, database, clientid, sspibuf, atchdbfile, changepwd) = do

    let hostnamebuf = encodeUcs2 hostname
        usernamebuf = encodeUcs2 username
        passwordbuf = manglePassword password
        appnamebuf = encodeUcs2 appname
        servernamebuf = encodeUcs2 servername
        ctlintnamebuf = encodeUcs2 ctlintname
        languagebuf = encodeUcs2 language
        databasebuf = encodeUcs2 database
        atchdbfilebuf = encodeUcs2 atchdbfile
        changepwdbuf = encodeUcs2 changepwd
        hostnameoff = 4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+6+4+4+4+4 :: Word16
        len = fromIntegral . B.length
        off prevoff prevbuf = prevoff + (len prevbuf)
        usernameoff = off hostnameoff hostnamebuf
        passwordoff = off usernameoff usernamebuf
        appnameoff = off passwordoff passwordbuf
        servernameoff = off appnameoff appnamebuf
        extensionoff = off servernameoff servernamebuf
        ctlintnameoff = off extensionoff extensionbuf
        languageoff = off ctlintnameoff ctlintnamebuf
        databaseoff = off languageoff languagebuf
        sspioff = off databaseoff databasebuf
        atchdbfileoff = off sspioff sspibuf
        changepwdoff = off atchdbfileoff atchdbfilebuf
        putSOffLen off buf = do
            putWord16le off
            putWord16le $ (len buf) `quot` 2
        putOffLen off buf = do
            putWord16le off
            putWord16le $ len buf
        putMacAddr (MacAddress b0 b1 b2 b3 b4 b5) = do
            putWord8 b0
            putWord8 b1
            putWord8 b2
            putWord8 b3
            putWord8 b4
            putWord8 b5
        putData = do
            putWord32le tdsver
            putWord32le packsize
            putWord32le clientver
            putWord32le pid
            putWord32le connid
            putWord8 optflags1
            putWord8 optflags2
            putWord8 typeflags
            putWord8 optflags3
            putWord32le clienttz
            putWord32le clientlcid
            putSOffLen hostnameoff hostnamebuf
            putSOffLen usernameoff usernamebuf
            putSOffLen passwordoff passwordbuf
            putSOffLen appnameoff appnamebuf
            putSOffLen servernameoff servernamebuf
            putOffLen extensionoff extensionbuf
            putSOffLen ctlintnameoff ctlintnamebuf
            putSOffLen languageoff languagebuf
            putSOffLen databaseoff databasebuf
            putMacAddr clientid
            putOffLen sspioff sspibuf
            putSOffLen atchdbfileoff atchdbfilebuf
            putSOffLen changepwdoff changepwdbuf
            putWord32le $ fromIntegral (B.length sspibuf)
            -- header done, putting variable part
            putLazyByteString hostnamebuf
            putLazyByteString usernamebuf
            putLazyByteString passwordbuf
            putLazyByteString appnamebuf
            putLazyByteString servernamebuf
            putLazyByteString extensionbuf
            putLazyByteString ctlintnamebuf
            putLazyByteString languagebuf
            putLazyByteString databasebuf
            putLazyByteString sspibuf
            putLazyByteString atchdbfilebuf
            putLazyByteString changepwdbuf

        databuf = runPut putData

    putWord32le $ fromIntegral (B.length databuf) + 4
    putLazyByteString databuf

tokenError = 170
tokenLoginAck = 173

getUsVarChar :: LG.Get String
getUsVarChar = do
    len <- LG.getWord16le
    buf <- LG.getLazyByteString ((fromIntegral len) * 2)
    return $ E.decodeLazyByteString UTF16LE buf

getBVarChar :: LG.Get String
getBVarChar = do
    len <- LG.getWord8
    buf <- LG.getLazyByteString ((fromIntegral len) * 2)
    return $ E.decodeLazyByteString UTF16LE buf

parseLoginAck = do
    LG.getWord16le
    iface <- LG.getWord8
    tdsver <- LG.getWord32be
    progname <- getBVarChar
    progver <- LG.getWord32be
    return $ TokLoginAck iface tdsver progname progver

parseError = do
    LG.getWord16le
    number <- LG.getWord32le
    state <- LG.getWord8
    cls <- LG.getWord8
    message <- getUsVarChar
    srvname <- getBVarChar
    procname <- getBVarChar
    lineno <- LG.getWord32le
    return $ TokError number state cls message srvname procname lineno


parseEnvChange = do
    let parseEnvChangeRec = do
            envtype <- LG.getWord8
            case envtype of
                4 -> do
                    curval <- getBVarChar
                    prevval <- getBVarChar
                    return $ PacketSize (read curval) (read prevval)

    size <- LG.getWord16le
    envchgrec <- parseEnvChangeRec
    return $ TokEnvChange [envchgrec]

parseDone = do
    status <- LG.getWord16le
    curcmd <- LG.getWord16le
    rowcount <- LG.getWord64le
    return $ TokDone status curcmd rowcount

parseToken :: LG.Get Token
parseToken = do
    tok <- LG.getWord8
    case tok of
        0 -> do
            return TokZero
        170 -> do
            parseError
        173 -> do
            parseLoginAck
        227 -> do
            parseEnvChange
        253 -> do
            parseDone
        _ -> do
            fail $ "unknown token type " ++ (show tok)

parseTokens :: LG.Get [Token]
parseTokens = do
    empty <- LG.isEmpty
    if empty
        then return []
        else do
            token <- parseToken
            tokens <- parseTokens
            return (token:tokens)


login = do
    hoststr <- getEnv "HOST"
    instances <- queryInstances hoststr
    --print (tokensToDictImpl tokens)
    inst <- getEnv "INSTANCE"
    password <- getEnv "SQLPASSWORD"
    username <- getEnv "SQLUSER"
    let ports = catMaybes [Map.lookup "tcp" i| i <- instances, isInst inst i]
    let port = htons (read (head ports) :: Word16)
    s <- Net.connectTo hoststr (Net.PortNumber $ Sock.PortNum port)
    -- sending prelogin request
    let instbytes = B.pack ((UTF8.encode inst) ++ [0])
    let prelogin = Map.fromList [(preloginVersion, B.pack [0, 0, 0, 0, 0, 0]),
                                 (preloginEncryption, B.pack [2]),
                                 (preloginInstOpt, instbytes),
                                 (preloginThreadId, B.pack [0, 0, 0, 0]),
                                 (preloginMars, B.pack [0])]
    let preloginbuf = runPut (serializePreLogin prelogin)
    let preloginpacket = runPut (serializePacket packPrelogin preloginbuf)
    B.hPutStr s preloginpacket
    -- reading prelogin response
    print "reading prelogin response"
    (packettype, _, preloginresppacket) <- getPacket s
    let preloginresp = LG.runGet getPreLoginHead preloginresppacket
    -- sending login request
    print "sending login"
    let login = (verTDS74, 4096, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                 "", username, password, "", "", B.empty, "", "", "",
                 (MacAddress 0 0 0 0 0 0), B.empty, "", "")
    let loginbuf = runPut $ serializeLogin login
    print loginbuf
    B.hPutStr s $ runPut (serializePacket packLogin7 loginbuf)
    -- reading login response
    print "reading login"
    --loginresppacket <- B.hGetNonBlocking s 4096
    (loginresptype, _, loginrespbuf) <- getPacket s
    let tokens = LG.runGet parseTokens loginrespbuf
    print tokens
    fail "not yet implemented"
