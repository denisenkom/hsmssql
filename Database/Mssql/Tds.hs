--module Main where
module Database.Mssql.Tds where

import Database.Mssql.Collation
import Database.HDBC
import Database.HDBC.Types
import qualified Network.Socket as Sock
import Data.Bits
import Data.Sequence((|>))
import qualified Data.Sequence as Seq
import Data.Word
import Data.Int
import Data.Ratio
import Network.Socket.ByteString
import qualified Data.Binary.Get as LG
import Data.Binary.Strict.Get
import Data.Binary.Put
import qualified Data.Encoding as E
import Data.Encoding.UTF16
import Data.Binary.IEEE754
--import Data.Text.Encoding
--import qualified Data.Text as T
import qualified Data.Map as Map
import qualified Codec.Binary.UTF8.String as UTF8
import Data.List.Split(splitOn)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as B
import System.IO
import Data.Maybe
import Control.Monad

foreign import ccall unsafe "htons" htons :: Word16 -> Word16

data MacAddress = MacAddress Word8 Word8 Word8 Word8 Word8 Word8

data Token = TokError {number :: Word32,
                       state :: Word8,
                       cls :: Word8,
                       message :: String,
                       srvname :: String,
                       procname :: String,
                       lineno :: Word32}
           | TokLoginAck {iface :: Word8,
                          tdsver :: Word32,
                          progname :: String,
                          progver :: Word32}
           | TokEnvChange [EnvChange]
           | TokDone Word16 Word16 Word64
           | TokZero
           | TokColMetaDataEmpty
           | TokColMetaData [ColMetaData] [Token]
           | TokRow [TdsValue]
     deriving(Eq, Show)

isTokLoginAck (TokLoginAck _ _ _ _) = True
isTokLoginAck _ = False

isTokError (TokError _ _ _ _ _ _ _) = True
isTokError _ = False

isTokMetaData (TokColMetaData _ _) = True
isTokMetaData TokColMetaDataEmpty = True
isTokMetaData _ = False

data EnvChange = PacketSize Int Int
     deriving(Eq, Show)

data TypeInfo = TypeNull
              | TypeInt1
              | TypeBit
              | TypeBitN Word8
              | TypeInt2
              | TypeInt4
              | TypeDateTim4
              | TypeFlt4
              | TypeMoney
              | TypeDateTime
              | TypeFlt8
              | TypeMoney4
              | TypeInt8
              | TypeGuid Word8
              | TypeIntN Word8
              | TypeDecimalN Word8 Word8
              | TypeNumericN Word8 Word8
              | TypeFltN Word8
              | TypeMoneyN Word8
              | TypeDateTimeN Word8
              | TypeDateN
              | TypeTimeN Word8
              | TypeDateTime2N Word8
              | TypeDateTimeOffsetN Word8
              | TypeVarBinary Word16
              | TypeVarChar Word16 Collation
              | TypeBinary Word16
              | TypeChar Word16 Collation
              | TypeNVarChar Word16 Collation
              | TypeNChar Word16 Collation
              | TypeXml
              | TypeUdt Word16
              | TypeText Int32 Collation
              | TypeImage Int32
              | TypeNText Int32 Collation
              | TypeVariant Int32
     deriving(Eq, Show)

data ColMetaData = ColMetaData Word32 Word16 TypeInfo String
     deriving(Eq, Show)

data TdsValue = TdsNull
              | TdsInt1 Word8
              | TdsInt2 Int16
              | TdsInt4 Int32
              | TdsInt8 Int64
              | TdsBool Bool
              | TdsFloat Double
              | TdsReal Float
              | TdsGuid BS.ByteString
              | TdsDecimal Word8 Word8 Rational
              | TdsMoney Int64
              | TdsSmallMoney Int32
              | TdsDateTime Int32 Word32
              | TdsSmallDateTime Word16 Word16
              | TdsDate Int32
              | TdsTime Rational
              | TdsDateTime2 Int32 Rational
              | TdsDateTimeOffset Int32 Rational Int16
              | TdsVarBinary BS.ByteString
              | TdsBinary BS.ByteString
              | TdsChar Collation BS.ByteString
              | TdsVarChar Collation BS.ByteString
              | TdsNChar Collation BS.ByteString
              | TdsNVarChar Collation BS.ByteString
              | TdsXml B.ByteString
              | TdsText Collation B.ByteString
              | TdsNText Collation B.ByteString
              | TdsImage B.ByteString
     deriving(Eq, Show)



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

packSQLBatch = 1
packLogin7 = 16
packPrelogin = 18

serializePacket :: Word8 -> B.ByteString -> Int -> Put
serializePacket packettype packet bufSize = do
    let (chunk, rem) = B.splitAt (fromIntegral (bufSize - 8)) packet
    putWord8 packettype
    putWord8 (if rem == B.empty then 1 else 0)  -- final packet
    putWord16be $ 8 + fromIntegral (B.length chunk)
    putWord16be 0 -- spid
    putWord8 0 -- packet no
    putWord8 0 -- padding
    putLazyByteString chunk
    if rem /= B.empty
        then serializePacket packettype rem bufSize
        else return ()
    flush

preparePreLoginImpl :: Int -> [(Word8, B.ByteString)] -> [(Word8, B.ByteString, Int)]
preparePreLoginImpl _ [] = []
preparePreLoginImpl offset [(k, v)] = [(k, v, offset)]
preparePreLoginImpl offset ((k, v):xs) = 
    let offset' = (offset + fromIntegral (B.length v))
    in (k, v, offset):(preparePreLoginImpl offset' xs)

preloginVersion = 0 :: Word8
preloginEncryption = 1 :: Word8
preloginInstOpt = 2 :: Word8
preloginThreadId = 3 :: Word8
preloginMars = 4 :: Word8
preloginTraceId = 5 :: Word8
preloginTerminator = 0xff :: Word8

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


sendPreLogin :: Handle -> Map.Map Word8 B.ByteString -> Int -> IO ()
sendPreLogin s prelogin bufSize =
    do let preloginbuf = runPut $ serializePreLogin prelogin
       B.hPutStr s $ runPut $ serializePacket packPrelogin preloginbuf bufSize


recvPreLogin :: Handle -> IO (Seq.Seq (Word8, Word16, Word16))
recvPreLogin s =
    do (packettype, preloginresppacket) <- getPacket s
       return $ LG.runGet getPreLoginHead preloginresppacket


getPacket :: Handle -> IO (Word8, B.ByteString)
getPacket s = do
    let decode = do
            packtype <- getWord8
            isfinal <- getWord8
            size <- getWord16be
            getWord16be  -- spid
            getWord8  -- packet no
            getWord8  -- padding
            return (packtype, isfinal /= 0, size)
        getStm = do
            hdrbuf <- BS.hGet s 8
            let (res, _) = runGet decode hdrbuf
            case res of
                Right (packtype, isfinal, size) -> do
                    content <- BS.hGet s ((fromIntegral size) - 8)
                    if isfinal
                        then return (packtype, [content])
                        else do
                            (_, tailChunks) <- getStm
                            return $ (packtype, content:tailChunks)
                Left err -> fail err
    (packtype, chunks) <- getStm
    return (packtype, B.fromChunks chunks)

getPreLoginHeadImpl :: Seq.Seq (Word8, Word16, Word16) -> LG.Get (Seq.Seq (Word8, Word16, Word16))
getPreLoginHeadImpl fields = do
    rectype <- LG.getWord8
    if rectype == preloginTerminator
        then return fields
        else do
            offset <- LG.getWord16be
            size <- LG.getWord16be
            getPreLoginHeadImpl $ fields |> (rectype, offset, size)

getPreLoginHead :: LG.Get (Seq.Seq (Word8, Word16, Word16))
getPreLoginHead = do
    getPreLoginHeadImpl Seq.empty

encodeUcs2 :: String -> B.ByteString
encodeUcs2 s = E.encodeLazyByteString UTF16LE s


manglePasswordByte :: Word8 -> Word8
manglePasswordByte ch = ((ch `shift` (-4)) .&. 0xff .|. (ch `shift` 4)) `xor` 0xA5

manglePassword :: String -> B.ByteString
manglePassword = (B.map manglePasswordByte) . encodeUcs2

verTDS74 = 0x74000004 :: Word32

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


sendLogin :: Handle -> (Word32, Word32, Word32, Word32, Word32,
                  Word8, Word8, Word8, Word8, Word32,
                  Word32, String, String, String, String,
                  String, B.ByteString, String, String, String,
                  MacAddress, B.ByteString, String,
                  String) -> Int -> IO ()
sendLogin s login bufSize =
    do let loginbuf = runPut $ serializeLogin login
       B.hPutStr s $ runPut (serializePacket packLogin7 loginbuf bufSize)


recvTokens s =
    do (loginresptype, loginrespbuf) <- getPacket s
       let tokens = LG.runGet parseTokens loginrespbuf
           errors = filter isTokError tokens
       return tokens


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

parseTypeInfo :: LG.Get TypeInfo
parseTypeInfo = do
    typeid <- LG.getWord8
    case typeid of
        0x1f -> return TypeNull
        0x22 -> do
            size <- LG.getWord32le
            numparts <- LG.getWord8
            forM_ [1..numparts] (\_ -> getUsVarChar)
            return $ TypeImage (fromIntegral size)
        0x23 -> do
            size <- LG.getWord32le
            col <- getCollation
            numparts <- LG.getWord8
            forM_ [1..numparts] (\_ -> getUsVarChar)
            return $ TypeText (fromIntegral size) col
        0x24 -> do
            size <- LG.getWord8
            return $ TypeGuid size
        -- 0x25 legacy varbinary
        0x26 -> do
            size <- LG.getWord8
            return $ TypeIntN size
        -- 0x27 legacy varchar
        0x28 -> return TypeDateN
        0x29 -> do
            scale <- LG.getWord8
            return $ TypeTimeN scale
        0x2a -> do
            scale <- LG.getWord8
            return $ TypeDateTime2N scale
        0x2b -> do
            scale <- LG.getWord8
            return $ TypeDateTimeOffsetN scale
        -- 0x2d legacy binary
        -- 0x2f legacy char
        0x30 -> return TypeInt1
        0x32 -> return TypeBit
        0x34 -> return TypeInt2
        0x38 -> return TypeInt4
        -- 0x37 legacy decimal
        0x3a -> return TypeDateTim4
        0x3b -> return TypeFlt4
        0x3c -> return TypeMoney
        0x3d -> return TypeDateTime
        0x3e -> return TypeFlt8
        0x62 -> do
            size <- LG.getWord32le
            return $ TypeVariant (fromIntegral size)
        0x63 -> do
            size <- LG.getWord32le
            col <- getCollation
            numparts <- LG.getWord8
            forM_ [1..numparts] (\_ -> getUsVarChar)
            return $ TypeNText (fromIntegral size) col
        0x68 -> do
            size <- LG.getWord8
            return $ TypeBitN size
        0x6a -> do
            size <- LG.getWord8
            prec <- LG.getWord8
            scale <- LG.getWord8
            return $ TypeDecimalN prec scale
        0x6c -> do
            size <- LG.getWord8
            prec <- LG.getWord8
            scale <- LG.getWord8
            return $ TypeDecimalN prec scale
        0x6d -> do
            size <- LG.getWord8
            return $ TypeFltN size
        0x6e -> do
            size <- LG.getWord8
            return $ TypeMoneyN size
        0x6f -> do
            size <- LG.getWord8
            return $ TypeDateTimeN size
        0x7a -> return TypeMoney4
        0x7f -> return TypeInt8
        0xa5 -> do
            size <- LG.getWord16le
            return $ TypeVarBinary size
        0xa7 -> do
            size <- LG.getWord16le
            col <- getCollation
            return $ TypeVarChar size col
        0xad -> do
            size <- LG.getWord16le
            return $ TypeBinary size
        0xaf -> do
            size <- LG.getWord16le
            col <- getCollation
            return $ TypeChar size col
        0xe7 -> do
            size <- LG.getWord16le
            col <- getCollation
            return $ TypeNVarChar size col
        0xef -> do
            size <- LG.getWord16le
            col <- getCollation
            return $ TypeNChar size col
        0xf0 -> do
            size <- LG.getWord16le
            return $ TypeUdt size
        0xf1 -> do
            schemapresent <- LG.getWord8
            if schemapresent /= 0
                then do
                    getBVarChar
                    getBVarChar
                    getUsVarChar
                    return ()
                else return ()
            return TypeXml
        otherwise -> fail ("unknown typeid: " ++ (show typeid))

parseRowCol :: ColMetaData -> LG.Get TdsValue
parseRowCol (ColMetaData _ _ ti _) = do
    case ti of
        TypeInt4 -> getInt4
        TypeGuid _ -> do
            size <- LG.getWord8
            if size == 0
                then return TdsNull
                else getGuid $ fromIntegral size
        TypeIntN _ -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                1 -> getInt1
                2 -> getInt2
                4 -> getInt4
                8 -> getInt8
        TypeBitN _ -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                1 -> getBit
        TypeFltN _ -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                4 -> getFlt4
                8 -> getFlt8
        TypeDecimalN prec scale -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                otherwise -> getDecimal prec scale (fromIntegral size)
        TypeNumericN prec scale -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                otherwise -> getDecimal prec scale (fromIntegral size)
        TypeMoneyN _ -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                4 -> getSmallMoney
                8 -> getMoney
        TypeDateTimeN _ -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                4 -> getSmallDateTime
                8 -> getDateTime
        TypeDateN -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                3 -> getDate
                    
        TypeTimeN scale -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                otherwise -> getTime (fromIntegral scale) (fromIntegral size)
        TypeDateTime2N scale -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                otherwise -> getDateTime2 (fromIntegral scale) (fromIntegral size)
        TypeDateTimeOffsetN scale -> do
            size <- LG.getWord8
            case size of
                0 -> return TdsNull
                otherwise -> getDateTimeOffset (fromIntegral scale) (fromIntegral size)
        TypeVarBinary size -> do
            size <- LG.getWord16le
            if size == 0xffff
                then return TdsNull
                else do
                    bs <- LG.getByteString (fromIntegral size)
                    return $ TdsVarBinary bs
        TypeBinary size -> do
            size <- LG.getWord16le
            if size == 0xffff
                then return TdsNull
                else do
                    bs <- LG.getByteString (fromIntegral size)
                    return $ TdsBinary bs
        TypeChar _ collation -> do
            size <- LG.getWord16le
            if size == 0xffff
                then return TdsNull
                else do
                    bs <- LG.getByteString (fromIntegral size)
                    return $ TdsChar collation bs
        TypeVarChar _ collation -> do
            size <- LG.getWord16le
            if size == 0xffff
                then return TdsNull
                else do
                    bs <- LG.getByteString (fromIntegral size)
                    return $ TdsVarChar collation bs
        TypeNChar _ collation -> do
            size <- LG.getWord16le
            if size == 0xffff
                then return TdsNull
                else do
                    bs <- LG.getByteString (fromIntegral size)
                    return $ TdsNChar collation bs
        TypeNVarChar _ collation -> do
            size <- LG.getWord16le
            if size == 0xffff
                then return TdsNull
                else do
                    bs <- LG.getByteString (fromIntegral size)
                    return $ TdsNVarChar collation bs
        TypeXml -> do
            size <- LG.getWord64le
            case size of
                0xffffffffffffffff -> return TdsNull
                otherwise -> do
                    bs <- getPlp
                    return $ TdsXml bs
        TypeText _ collation -> do
            size <- LG.getWord8
            if size == 0
                then return TdsNull
                else do
                    LG.getByteString (fromIntegral size)  -- textptr
                    LG.getByteString 8  -- timestamp
                    colSize <- LG.getWord32le
                    bs <- LG.getLazyByteString (fromIntegral colSize)
                    return $ TdsText collation bs
        TypeNText _ collation -> do
            size <- LG.getWord8
            if size == 0
                then return TdsNull
                else do
                    LG.getByteString (fromIntegral size)  -- textptr
                    LG.getByteString 8  -- timestamp
                    colSize <- LG.getWord32le
                    bs <- LG.getLazyByteString (fromIntegral colSize)
                    return $ TdsNText collation bs
        TypeImage _ -> do
            size <- LG.getWord8
            if size == 0
                then return TdsNull
                else do
                    LG.getByteString (fromIntegral size)  -- textptr
                    LG.getByteString 8  -- timestamp
                    colSize <- LG.getWord32le
                    bs <- LG.getLazyByteString (fromIntegral colSize)
                    return $ TdsImage bs
        TypeVariant _ -> do
            size <- LG.getWord32le
            if size == 0
                then return TdsNull
                else do
                    typeId <- LG.getWord8
                    propBytes <- LG.getWord8
                    let dataSize = (fromIntegral size) - (fromIntegral propBytes) - 2
                    case typeId of
                        0x24 -> getGuid dataSize
                        0x28 -> getDate
                        0x29 -> do
                            scale <- LG.getWord8
                            getTime (fromIntegral scale) dataSize
                        0x2a -> do
                            scale <- LG.getWord8
                            getDateTime2 (fromIntegral scale) dataSize
                        0x2b -> do
                            scale <- LG.getWord8
                            getDateTimeOffset (fromIntegral scale) dataSize
                        0x30 -> getInt1
                        0x32 -> getBit
                        0x34 -> getInt2
                        0x38 -> getInt4
                        0x3C -> getMoney
                        0x3D -> getDateTime
                        0x3A -> getSmallDateTime
                        0x3b -> getFlt4
                        0x3e -> getFlt8
                        0x6a -> do
                            prec <- LG.getWord8
                            scale <- LG.getWord8
                            getDecimal prec scale dataSize
                        0x6c -> do
                            prec <- LG.getWord8
                            scale <- LG.getWord8
                            getDecimal prec scale dataSize
                        0x7a -> getSmallMoney
                        0x7f -> getInt8
                        0xa5 -> do
                            LG.getWord16le
                            bs <- LG.getByteString dataSize
                            return $ TdsVarBinary bs
                        0xa7 -> do
                            collation <- getCollation
                            LG.getWord16le
                            bs <- LG.getByteString dataSize
                            return $ TdsVarChar collation bs
                        0xad -> do
                            LG.getWord16le
                            bs <- LG.getByteString dataSize
                            return $ TdsBinary bs
                        0xaf -> do
                            collation <- getCollation
                            LG.getWord16le
                            bs <- LG.getByteString dataSize
                            return $ TdsChar collation bs
                        0xe7 -> do
                            collation <- getCollation
                            LG.getWord16le
                            bs <- LG.getByteString dataSize
                            return $ TdsNVarChar collation bs
                        0xef -> do
                            collation <- getCollation
                            LG.getWord16le
                            bs <- LG.getByteString dataSize
                            return $ TdsNChar collation bs


getFlt4 = do
    val <- getFloat32le
    return $ TdsReal val

getFlt8 = do
    val <- getFloat64le
    return $ TdsFloat val

getSmallDateTime = do
    days <- LG.getWord16le
    minutes <- LG.getWord16le
    return $ TdsSmallDateTime days minutes

getDateTime = do
    days <- LG.getWord32le
    timefrac <- LG.getWord32le
    return $ TdsDateTime (fromIntegral days) timefrac

getSmallMoney = do
    val <- LG.getWord32le
    return $ TdsSmallMoney (fromIntegral val)

getMoney = do
    hi <- LG.getWord32le
    lo <- LG.getWord32le
    return $ TdsMoney (((fromIntegral hi) `shiftL` 32) + (fromIntegral lo))

getInt1 = do
    val <- LG.getWord8
    return $ TdsInt1 val


getInt2 = do
    val <- LG.getWord16le
    return $ TdsInt2 (fromIntegral val)


getInt8 = do
    val <- LG.getWord64le
    return $ TdsInt8 (fromIntegral val)


getBit = do
    val <- LG.getWord8
    return $ TdsBool (if val == 0 then False else True)


getGuid size = do
    bs <- LG.getByteString (fromIntegral size)
    return $ TdsGuid bs


getInt4 = do
    val <- LG.getWord32le
    return $ TdsInt4 (fromIntegral val)


getPlp :: LG.Get B.ByteString
getPlp = getChunks
    where getChunks = do
            chunkSize <- LG.getWord32le
            if chunkSize == 0
                then return B.empty
                else do
                    chunk <- LG.getLazyByteString (fromIntegral chunkSize)
                    tailChunks <- getChunks
                    return $ B.append chunk tailChunks

getDateTimeOffset :: Int -> Int -> LG.Get TdsValue
getDateTimeOffset scale size = do
    secs <- getTimeSecs (fromIntegral scale) ((fromIntegral size) - 5)
    days <- getDateDays
    offset <- LG.getWord16le
    return $ TdsDateTimeOffset days secs (fromIntegral offset)

getDateTime2 :: Int -> Int -> LG.Get TdsValue
getDateTime2 scale size = do
    secs <- getTimeSecs (fromIntegral scale) ((fromIntegral size) - 3)
    days <- getDateDays
    return $ TdsDateTime2 days secs

getTime :: Int -> Int -> LG.Get TdsValue
getTime scale size = do
    secs <- getTimeSecs (fromIntegral scale) (fromIntegral size)
    return $ TdsTime secs

getTimeSecs :: Int -> Int -> LG.Get Rational
getTimeSecs scale size = do
    ints <- replicateM size LG.getWord8
    let time = foldr (\v acc -> (fromIntegral v) + (acc `shiftL` 8)) (0 :: Word64) ints
    return $ (fromIntegral time) % (10 ^ scale)

getDate = do
    days <- getDateDays
    return $ TdsDate days

getDateDays :: LG.Get Int32
getDateDays = do
    b1 <- LG.getWord8
    b2 <- LG.getWord8
    b3 <- LG.getWord8
    let days = ((fromIntegral b1) + (fromIntegral b2) * 256 +
                (fromIntegral b3) * 256 * 256)
    return days

decimalFold :: [Word32] -> Integer
decimalFold = foldr (\v acc -> (fromIntegral v) + (acc `shiftL` 32)) 0

getDecimal prec scale size = do
    sign <- LG.getWord8
    ints <- replicateM ((size - 1) `quot` 4) LG.getWord32le
    let rational = (decimalFold ints) % (10 ^ scale)
        res = if sign == 0 then (-rational) else rational
    return $ TdsDecimal prec scale res

parseRowHelper :: [ColMetaData] -> LG.Get [TdsValue]
parseRowHelper [] = return []
parseRowHelper (col:xs) = do
    val <- parseRowCol col
    vals <- parseRowHelper xs
    return $ val:vals

parseRowM :: [ColMetaData] -> LG.Get (Maybe Token)
parseRowM cols = do
    tok <- LG.getWord8
    case tok of
        209 -> do
            vals <- parseRowHelper cols
            return $ Just (TokRow vals)
        otherwise -> return Nothing

parseRows :: [ColMetaData] -> LG.Get [Token]
parseRows cols = do
    rowm <- LG.lookAheadM $ parseRowM cols
    case rowm of
        Just row -> do
            rows <- parseRows cols
            return (row:rows)
        Nothing -> return []

parseColMetaData72 :: LG.Get Token
parseColMetaData72 = do
    let parseCol = do
            usertype <- LG.getWord32le
            flags <- LG.getWord16le
            ti <- parseTypeInfo
            name <- getBVarChar
            return $ ColMetaData usertype flags ti name
        parseMeta 0xffff = return TokColMetaDataEmpty
        parseMeta cnt = do
            cols <- parseCols cnt
            rows <- parseRows cols
            return $ TokColMetaData cols rows
        parseCols 0 = return []
        parseCols cnt = do
            col <- parseCol
            cols <- parseCols $ cnt - 1
            return (col:cols)

    cnt <- LG.getWord16le
    parseMeta cnt


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
        129 -> do
            parseColMetaData72
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


getPort host inst = do
    if inst /= ""
        then do
            instances <- queryInstances host
            let ports = catMaybes [Map.lookup "tcp" i| i <- instances, isInst inst i]
            if ports == []
                then fail ("Instance " ++ inst ++ " not found")
                else do
                    let port = (read (head ports) :: Word16)
                    return port
        else return 1433


dataStmHdrQueryNotif = 1  -- query notifications
dataStmHdrTransDescr = 2  -- MARS transaction descriptor (required)
dataStmHdrTraceActivity = 3

data DataStmHeader = DataStmHeader Word16 B.ByteString
            | DataStmTransDescrHdr Word64 Word32


putDataStmHeaders headers = do
    let putHeader (DataStmHeader htype dat) = do
            putWord16le htype
            putLazyByteString dat
        putHeader (DataStmTransDescrHdr transDescr reqCnt) = do
            putWord16le dataStmHdrTransDescr
            putWord64le transDescr
            putWord32le reqCnt
        putHeaders [] = return ()
        putHeaders (header:xs) = do
            let hdrbuf = runPut $ putHeader header
                hdrsize = fromIntegral (B.length hdrbuf) + 4
            putWord32le hdrsize
            putLazyByteString hdrbuf
            putHeaders xs
        headersbuf = runPut $ putHeaders headers
        headerssize = fromIntegral (B.length headersbuf) + 4

    putWord32le headerssize
    putLazyByteString headersbuf




sendSqlBatch72 :: Handle -> [DataStmHeader] -> String -> Int -> IO ()
sendSqlBatch72 s headers query bufSize = do
    let putContent = do
            putDataStmHeaders headers
            putLazyByteString $ encodeUcs2 query
        packetbuf = runPut putContent
    B.hPutStr s $ runPut $ serializePacket packSQLBatch packetbuf bufSize


exec :: Handle -> String -> Int -> IO [Token]
exec s query bufSize = do
    let headers = [DataStmTransDescrHdr 0 1]
    sendSqlBatch72 s headers query bufSize
    (resptype, respbuf) <- getPacket s
    let tokens = LG.runGet parseTokens respbuf
        errors = filter isTokError tokens
    return tokens
