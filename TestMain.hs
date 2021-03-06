{-# OPTIONS_GHC -F -pgmF htfpp #-}
module Main where

import Control.Exception
import Database.Mssql.Tds
import Database.Mssql.Connection
import Database.HDBC
import Data.Binary.Put
import Data.Binary.Strict.Get
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as B
import qualified Data.Map as M
import Data.Char (ord)
import Data.Maybe
import Data.Ratio
import Data.String.Utils
import Data.Time.Calendar
import Data.Time.LocalTime
import System.Environment
import Test.Framework
import Test.HUnit.Tools

test_getInstances =
    let s = "\ENQ\179\NULServerName;sqlhost;InstanceName;SQLEXPRESS;" ++
            "IsClustered;No;Version;10.0.1600.22;tcp;49849;;" ++
            "ServerName;sqlhost;InstanceName;SQL2012;IsClustered;" ++
            "No;Version;11.0.2100.60;tcp;59958;;"
        bs = BS.pack $ map (fromIntegral . ord) s
        ref = [
            M.fromList [("InstanceName","SQL2012"),
                        ("IsClustered","No"),
                        ("ServerName","sqlhost"),
                        ("Version","11.0.2100.60"),
                        ("tcp","59958")],
            M.fromList [("InstanceName","SQLEXPRESS"),
                        ("IsClustered","No"),
                        ("ServerName","sqlhost"),
                        ("Version","10.0.1600.22"),
                        ("tcp","49849")]]
        (decoded, _) = runGet getInstances bs
    in
        assertEqual (Right ref) decoded

test_putPacket =
    let buf = runPut $ putPacket 1 (B.replicate 10 10) 16
        ref = B.pack [1, 0, 0, 16, 0, 0, 0, 0, 10, 10, 10, 10, 10, 10, 10, 10,
                      1, 1, 0, 10, 0, 0, 0, 0, 10, 10]
    in
        assertEqual ref buf


test_sendLogin =
    let login = (verTDS74,
                 0x1000,
                 0x01060100,
                 100,
                 0,
                 0xe0,
                 0,
                 0,
                 8,
                 -4 * 60,
                 0x204,
                 "subdev1",
                 "test",
                 "testpwd",
                 "appname",
                 "servername",
                 B.empty,
                 "library",
                 "en",
                 "database",
                 (MacAddress 0x12 0x34 0x56 0x78 0x90 0xab),
                 B.empty,
                 "filepath",
                 "")
        loginbuf = runPut $ putLogin login
        packet = runPut $ putPacket packLogin7 loginbuf 4096
        ref = [
            16, 1, 0, 222, 0, 0, 0, 0, 198+16, 0, 0, 0, 4, 0, 0, 116, 0, 16, 0, 0, 0, 1,
            6, 1, 100, 0, 0, 0, 0, 0, 0, 0, 224, 0, 0, 8, 16, 255, 255, 255, 4, 2, 0,
            0, 94, 0, 7, 0, 108, 0, 4, 0, 116, 0, 7, 0, 130, 0, 7, 0, 144, 0, 10, 0, 164,
            0, 0, 0, 164, 0, 7, 0, 178, 0, 2, 0, 182, 0, 8, 0, 18, 52, 86, 120, 144, 171,
            198, 0, 0, 0, 198, 0, 8, 0, 214, 0, 0, 0, 0, 0, 0, 0, 115, 0, 117, 0, 98,
            0, 100, 0, 101, 0, 118, 0, 49, 0, 116, 0, 101, 0, 115, 0, 116, 0, 226, 165,
            243, 165, 146, 165, 226, 165, 162, 165, 210, 165, 227, 165, 97, 0, 112,
            0, 112, 0, 110, 0, 97, 0, 109, 0, 101, 0, 115, 0, 101, 0, 114, 0, 118, 0,
            101, 0, 114, 0, 110, 0, 97, 0, 109, 0, 101, 0, 108, 0, 105, 0, 98, 0, 114,
            0, 97, 0, 114, 0, 121, 0, 101, 0, 110, 0, 100, 0, 97, 0, 116, 0, 97, 0, 98,
            0, 97, 0, 115, 0, 101, 0, 102, 0, 105, 0, 108, 0, 101, 0, 112, 0, 97, 0,
            116, 0, 104, 0]
    in
        assertEqual ref $ B.unpack packet

connect = do
    hoststr <- getEnv "HOST"
    inst <- fmap (fromMaybe "") (lookupEnv "INSTANCE")
    password <- fmap (fromMaybe "sa") (lookupEnv "SQLPASSWORD")
    username <- fmap (fromMaybe "sa") (lookupEnv "SQLUSER")
    connectMssql hoststr inst username password

test_connect = do
    conn <- connect
    disconnect conn

test_badPwd = do
    hoststr <- getEnv "HOST"
    inst <- fmap (fromMaybe "") (lookupEnv "INSTANCE")
    password <- fmap (fromMaybe "sa") (lookupEnv "SQLPASSWORD")
    username <- fmap (fromMaybe "sa") (lookupEnv "SQLUSER")
    let handler :: SomeException -> IO ()
        handler e = return ()
        doConnect = connectMssql hoststr inst (username ++ "bad") (password ++ "bad")
        try = do
            doConnect
            fail "Should fail with bad password"
    try `catch` handler

test_runRaw = do
    conn <- connect
    runRaw conn "select 1"
    disconnect conn

test_statement = do
    conn <- connect
    stm <- prepare conn "select 1 as fld1, 2 as fld2"
    executeRaw stm
    names <- getColumnNames stm
    assertEqual ["fld1", "fld2"] names
    descr <- describeResult stm
    assertEqual [("fld1", SqlColDesc SqlIntegerT Nothing Nothing Nothing (Just False)),
                 ("fld2", SqlColDesc SqlIntegerT Nothing Nothing Nothing (Just False))]
                descr
    rows <- fetchAllRows stm
    assertEqual [[SqlInt32 1, SqlInt32 2]] rows

    stm <- prepare conn "select @p1"
    execute stm [SqlInt32 1]
    rows <- fetchAllRows stm
    assertEqual [[SqlInt32 1]] rows


test_bigRequest = do
    conn <- connect
    let val = (replicate 4000 'x')
    stm <- prepare conn ("select len('" ++ val ++ "')")
    executeRaw stm
    rows <- fetchAllRows stm
    assertEqual [[SqlInt32 (fromIntegral (length val))]] rows

test_error = do
    conn <- connect
    let ex = SqlError {seState = "",
                       seNativeError = 156,
                       seErrorMsg = "Incorrect syntax near the keyword 'where'."}
    assertRaises "" ex $ runRaw conn "select where from"

    stm <- prepare conn "select where from"
    assertRaises "" ex $ executeRaw stm

test_types = do
    conn <- connect
    let tests = [("1.5", "float", SqlDouble 1.5),
                 ("1.5", "real", SqlDouble 1.5),
                 ("3", "bigint", SqlInt64 3),
                 ("-9223372036854775808", "bigint", SqlInt64 (-9223372036854775808)),
                 ("9223372036854775807", "bigint", SqlInt64 9223372036854775807),
                 ("3", "int", SqlInt32 3),
                 ("-2147483648", "int", SqlInt32 (-2147483648)),
                 ("2147483647", "int", SqlInt32 2147483647),
                 ("3", "smallint", SqlInt32 3),
                 ("-32768", "smallint", SqlInt32 (-32768)),
                 ("32767", "smallint", SqlInt32 32767),
                 ("3", "tinyint", SqlInt32 3),
                 ("0", "tinyint", SqlInt32 0),
                 ("255", "tinyint", SqlInt32 255),
                 ("NULL", "uniqueidentifier", SqlNull),
                 ("'0E984725-C51C-4BF4-9960-E1C80E27ABA0'", "uniqueidentifier", SqlByteString (BS.pack [0x25,0x47,0x98,0x0E, 0x1C,0xC5,0xF4,0x4B, 0x99,0x60,0xE1,0xC8,0x0E,0x27,0xAB,0xA0])),
                 ("0", "bit", SqlBool False),
                 ("1", "bit", SqlBool True),
                 ("100", "money", SqlRational 100),
                 ("-922337203685477.5808", "money", SqlRational (-922337203685477.5808)),
                 ("922337203685477.5807", "money", SqlRational 922337203685477.5807),
                 ("100.1234", "smallmoney", SqlRational 100.1234),
                 ("-214748.3648", "smallmoney", SqlRational (-214748.3648)),
                 ("214748.3647", "smallmoney", SqlRational 214748.3647),
                 ("'2010-01-02T03:04:05.010'", "datetime",
                  SqlLocalTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 5.01))),
                 ("'1753-01-01T00:00:00.000'", "datetime",
                  SqlLocalTime (LocalTime (fromGregorian 1753 1 1)
                                          (TimeOfDay 0 0 0))),
                 ("'9999-12-31T23:59:59.997'", "datetime",
                  SqlLocalTime (LocalTime (fromGregorian 9999 12 31)
                                          (TimeOfDay 23 59 59.996666666667))),
                 ("'2010-01-02T03:04:00'", "smalldatetime",
                  SqlLocalTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 0))),
                 ("'1900-01-01T00:00:00'", "smalldatetime",
                  SqlLocalTime (LocalTime (fromGregorian 1900 1 1)
                                          (TimeOfDay 0 0 0))),
                 ("'2079-06-06T23:59:00'", "smalldatetime",
                  SqlLocalTime (LocalTime (fromGregorian 2079 6 6)
                                          (TimeOfDay 23 59 0))),
                 ("NULL", "date", SqlNull),
                 ("'2014-01-01'", "date",
                  SqlLocalDate $ fromGregorian 2014 1 1),
                 ("'0001-01-01'", "date",
                  SqlLocalDate $ fromGregorian 1 1 1),
                 ("'9999-12-31'", "date",
                  SqlLocalDate $ fromGregorian 9999 12 31),
                 ("NULL", "time", SqlNull),
                 ("'01:02:03.4567891'", "time(7)", SqlLocalTimeOfDay (TimeOfDay 1 2 3.4567891)),
                 ("'00:00:00'", "time(7)", SqlLocalTimeOfDay (TimeOfDay 0 0 0)),
                 ("'23:59:59.9999999'", "time(7)", SqlLocalTimeOfDay (TimeOfDay 23 59 59.9999999)),
                 ("'2010-01-02T03:04:05.010'", "datetime2",
                  SqlLocalTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 5.01))),
                 ("'2010-01-02T03:04:05.010+05:30'", "datetimeoffset",
                  SqlZonedTime (ZonedTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 5.01))
                                          (minutesToTimeZone (5 * 60 + 30)))),
                 ("'2010-01-02T03:04:05.010-05:30'", "datetimeoffset",
                  SqlZonedTime (ZonedTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 5.01))
                                          (minutesToTimeZone (-5 * 60 - 30)))),
                 ("0x123456", "varbinary(3)",
                  SqlByteString (BS.pack [0x12,0x34,0x56])),
                 ("0x123456", "varbinary(max)",
                  SqlByteString (BS.pack [0x12,0x34,0x56])),
                 ("0x123456", "binary(3)",
                  SqlByteString (BS.pack [0x12,0x34,0x56])),
                 ("'abc'", "char(3)", SqlString "abc"),
                 ("'abcd'", "varchar(4)", SqlString "abcd"),
                 ("'abcd'", "varchar(max)", SqlString "abcd"),
                 ("N'abcd'", "nchar(4)", SqlString "abcd"),
                 ("N'abcd'", "nvarchar(4)", SqlString "abcd"),
                 ("N'abcd'", "nvarchar(max)", SqlString "abcd"),
                 ("'<root/>'", "xml", SqlString "<root/>"),
                 ("'abcd'", "text", SqlString "abcd"),
                 ("'abcd'", "ntext", SqlString "abcd"),
                 ("0x123456", "image", SqlByteString (BS.pack [0x12,0x34,0x56])),
                 ("NULL", "sql_variant", SqlNull),
                 ("1", "sql_variant", SqlInt32 1),
                 ("cast('0E984725-C51C-4BF4-9960-E1C80E27ABA0' as uniqueidentifier)",
                  "sql_variant", SqlByteString (BS.pack [0x25,0x47,0x98,0x0E, 0x1C,0xC5,0xF4,0x4B, 0x99,0x60,0xE1,0xC8,0x0E,0x27,0xAB,0xA0])),
                 ("cast(1 as bit)", "sql_variant", SqlBool True),
                 ("cast(5 as tinyint)", "sql_variant", SqlInt32 5),
                 ("cast(6 as smallint)", "sql_variant", SqlInt32 6),
                 ("cast(7 as bigint)", "sql_variant", SqlInt64 7),
                 ("cast(100 as money)", "sql_variant", SqlRational 100),
                 ("cast(100 as smallmoney)", "sql_variant", SqlRational 100),
                 ("cast('2010-01-02T03:04:05.010'as datetime)", "sql_variant",
                  SqlLocalTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 5.01))),
                 ("cast('2010-01-02T03:04:00' as smalldatetime)", "sql_variant",
                  SqlLocalTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 0))),
                 ("cast(1.5 as float)", "sql_variant", SqlDouble 1.5),
                 ("cast(1.5 as real)", "sql_variant", SqlDouble 1.5),
                 ("cast('2014-01-01' as date)", "sql_variant",
                  SqlLocalDate $ fromGregorian 2014 1 1),
                 ("cast('01:02:03.4567891' as time(7))", "sql_variant",
                  SqlLocalTimeOfDay (TimeOfDay 1 2 3.4567891)),
                 ("cast('2010-01-02T03:04:05.010' as datetime2)", "sql_variant",
                  SqlLocalTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 5.01))),
                 ("cast('2010-01-02T03:04:05.010+05:30' as datetimeoffset)",
                  "sql_variant",
                  SqlZonedTime (ZonedTime (LocalTime (fromGregorian 2010 1 2)
                                          (TimeOfDay 3 4 5.01))
                                          (minutesToTimeZone (5 * 60 + 30)))),
                 ("cast(0x123456 as varbinary(3))", "sql_variant",
                  SqlByteString (BS.pack [0x12,0x34,0x56])),
                 ("cast(0x123456 as binary(3))", "sql_variant",
                  SqlByteString (BS.pack [0x12,0x34,0x56])),
                 ("cast('1.5' as decimal(10,1))", "sql_variant", SqlRational 1.5),
                 ("cast('1.5' as numeric(10,1))", "sql_variant", SqlRational 1.5),
                 ("cast('abc' as char(3))", "sql_variant", SqlString "abc"),
                 ("cast('abcd' as varchar(4))", "sql_variant", SqlString "abcd"),
                 ("cast(N'abc' as nchar(3))", "sql_variant", SqlString "abc"),
                 ("cast(N'abcd' as nvarchar(4))", "sql_variant", SqlString "abcd"),
                 ("'1.5'", "decimal(10,1)", SqlRational 1.5),
                 ("'1.1234'", "decimal(10,4)", SqlRational 1.1234),
                 ("'-100'", "decimal(38)", SqlRational (-100)),
                 ("'" ++ (show (10 ^ 38 - 1)) ++ "'", "decimal(38)", SqlRational (10 ^ 38 - 1)),
                 ("'" ++ (show (-10 ^ 38 + 1)) ++ "'", "decimal(38)", SqlRational (-10 ^ 38 + 1)),
                 ("'1.1234'", "numeric(10,4)", SqlRational 1.1234)]
        query = "select " ++ (join "," [("cast(" ++ sql ++ " as " ++ sqltype ++ ")") | (sql, sqltype, _) <- tests])
        values = [val | (_, _, val) <- tests]
    stm <- prepare conn query
    executeRaw stm
    rows <- fetchAllRows stm
    assertEqual [values] rows

test_parameterTypes = do
    let zonedTime = ZonedTime (LocalTime (fromGregorian 2010 1 2)
                                         (TimeOfDay 3 4 5.01))
                              (minutesToTimeZone $ 5 * 60 + 30)
    let values = [(SqlString "hello", SqlString "hello"),
                  (SqlByteString $ BS.pack [1,2,3], SqlByteString $ BS.pack [1,2,3]),
                  (SqlWord32 100, SqlInt64 100),
                  (SqlWord64 100, SqlRational 100),
                  (SqlInt32 1, SqlInt32 1),
                  (SqlInt64 100, SqlInt64 100),
                  (SqlInteger 777, SqlRational 777),
                  (SqlChar 'x', SqlString "x"),
                  (SqlBool True, SqlBool True),
                  (SqlDouble 0.25, SqlDouble 0.25),
                  (SqlRational 1000, SqlRational 1000),
                  (SqlLocalDate $ fromGregorian 2014 1 1, SqlLocalDate $ fromGregorian 2014 1 1),
                  (SqlLocalTimeOfDay $ TimeOfDay 1 2 3.1234567, SqlLocalTimeOfDay $ TimeOfDay 1 2 3.1234567),
                  (SqlLocalTime $ LocalTime (fromGregorian 2010 1 2) (TimeOfDay 3 4 5.01),
                   SqlLocalTime $ LocalTime (fromGregorian 2010 1 2) (TimeOfDay 3 4 5.01)),
                  (SqlZonedTime zonedTime, SqlZonedTime zonedTime),
                  (SqlNull, SqlNull)
                  ]
        sql = "select " ++ join "," ["@p" ++ show n | n <- [1..length values]]

    conn <- connect
    stm <- prepare conn sql
    execute stm [v | (v, _) <- values]
    rows <- fetchAllRows stm
    assertEqual [[v | (_, v) <- values]] rows

test_describeResult = do
    let tests = [("float", (SqlDoubleT, Nothing, Nothing, Nothing)),
                 ("real", (SqlFloatT, Nothing, Nothing, Nothing)),
                 ("numeric(10,4)", (SqlDecimalT, Just 10, Nothing, Just 4))]
        query = "select " ++ (join "," [("cast(null as " ++ sqltype ++ ")") | (sqltype, _) <- tests])
        values = [("", SqlColDesc typ size octlen dec (Just True)) | (_, (typ, size, octlen, dec)) <- tests]
    conn <- connect
    stm <- prepare conn query
    executeRaw stm
    descr <- describeResult stm
    assertEqual values descr

main = htfMain htf_thisModulesTests
