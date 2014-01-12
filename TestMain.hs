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
import System.Environment
import Test.Framework

test_parseInstances =
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
        decoded = parseInstances bs
    in
        assertEqual (Right ref) decoded

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
        loginbuf = runPut $ serializeLogin login
        packet = runPut $ serializePacket packLogin7 loginbuf
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
    password <- getEnv "SQLPASSWORD"
    username <- getEnv "SQLUSER"
    connectMssql hoststr inst username password

test_connect = do
    conn <- connect
    disconnect conn

test_badPwd = do
    hoststr <- getEnv "HOST"
    inst <- fmap (fromMaybe "") (lookupEnv "INSTANCE")
    password <- getEnv "SQLPASSWORD"
    username <- getEnv "SQLUSER"
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
    rows <- fetchAllRows stm
    assertEqual [[SqlInt32 1, SqlInt32 2]] rows

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
                 ("'0E984725-C51C-4BF4-9960-E1C80E27ABA0'", "uniqueidentifier", SqlByteString (BS.pack [0x25,0x47,0x98,0x0E, 0x1C,0xC5,0xF4,0x4B, 0x99,0x60,0xE1,0xC8,0x0E,0x27,0xAB,0xA0])),
                 ("0", "bit", SqlBool False),
                 ("1", "bit", SqlBool True)]
        runTests [] = return ()
        runTests ((sql, sqltype, val):xs) = do
            stm <- prepare conn ("select cast(" ++ sql ++ " as " ++ sqltype ++ ")")
            executeRaw stm
            rows <- fetchAllRows stm
            assertEqual [[val]] rows
            runTests xs

    runTests tests


main = htfMain htf_thisModulesTests
