{-# LANGUAGE ExistentialQuantification #-}
module Database.Mssql.Collation where

import qualified Data.Binary.Get as LG
import qualified Data.Binary.Put as LP
import Data.Bits
import Data.Encoding hiding(DynEncoding)
import Data.Encoding.CP437
import Data.Encoding.CP1250
import Data.Encoding.CP1251
import Data.Typeable

data Collation = Collation {sortId :: Int,
                            lcidAndFlags :: Int}
     deriving(Eq, Show)

data DynEncoding = forall enc. (Encoding enc,Eq enc,Typeable enc,Show enc) => DynEncoding enc

instance Show DynEncoding where
    show (DynEncoding enc) = show enc

instance Encoding DynEncoding where
    decodeChar (DynEncoding e) = decodeChar e
    encodeChar (DynEncoding e) = encodeChar e
    decode (DynEncoding e) = decode e
    encode (DynEncoding e) = encode e
    encodeable (DynEncoding e) = encodeable e

instance Eq DynEncoding where
    (DynEncoding e1) == (DynEncoding e2) = case cast e2 of
                                             Nothing -> False
                                             Just e2' -> e1==e2'

emptyCollation = Collation 0 0

getCollation :: LG.Get Collation
getCollation = do
    lcidandflags <- LG.getWord32le
    sortid <- LG.getWord8
    return $ Collation {sortId = fromIntegral sortid,
                        lcidAndFlags = fromIntegral lcidandflags}

putCollation :: Collation -> LP.Put
putCollation coll = do
    LP.putWord32le $ fromIntegral $ lcidAndFlags coll
    LP.putWord8 $ fromIntegral $ sortId coll

charSetFromSortId :: Int -> DynEncoding
charSetFromSortId sortId =
    case sortId of
        30 -> DynEncoding CP437

charSetFromLcid :: Int -> DynEncoding
charSetFromLcid lcid =
    case lcid of
        0x402 -> DynEncoding CP1251
        0x405 -> DynEncoding CP1250
        0x419 -> DynEncoding CP1251
        otherwise -> error ("lcid " ++ (show lcid) ++ " not implemented")

getCharSet :: Collation -> DynEncoding
getCharSet (Collation sortId lcidAndFlags) = 
    if sortId == 0
        then charSetFromLcid (lcidAndFlags .&. 0xfffff)
        else charSetFromSortId sortId
