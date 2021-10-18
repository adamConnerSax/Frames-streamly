{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
module Frames.Streamly.OrMissing
  (
    OrMissing(..)
  , toMaybe
  , toOrMissing
  , orMissing
  , derivingOrMissingUnboxVectorFor
  , derivingOrMissingUnboxVectorFor'
  -- * re-export derivingUnbox for ergonomics
  , derivingUnbox
    -- * re-exports for deriving
  , module Data.Vector.Unboxed -- for Vector
  , module Frames.Streamly.InCore -- for VectorFor
  )
where

import Frames.Streamly.InCore as FStreamly
import Frames.Streamly.InCore (VectorFor)

import Prelude hiding (Type, lift)
import qualified Data.Text as T
import Data.Vector.Unboxed.Deriving
import Data.Vector.Unboxed (Unbox)
import qualified Data.Vector as Vec
import qualified Data.Vector.Unboxed as UVec
import Data.Vector.Unboxed (Vector)
import Language.Haskell.TH

-- | Represent data that may be present or missing in a data-set, but is definitely of type @a@
-- This is isomorphic to Maybe but a different type is used to avoid orphan instances for
-- @Unbox@ and type instance @VectorFor@
data OrMissing a = Missing | Present a deriving (Show, Eq, Ord, Generic, Functor, Typeable)

toMaybe :: OrMissing a -> Maybe a
toMaybe Missing = Nothing
toMaybe (Present a) = Just a
{-# INLINE toMaybe #-}

toOrMissing :: Maybe a -> OrMissing a
toOrMissing Nothing = Missing
toOrMissing (Just a) = Present a
{-# INLINE toOrMissing #-}

orMissing :: a -> (b -> a) -> OrMissing b -> a
orMissing defA toA = \case
  Missing -> defA
  Present b -> toA b
{-# INLINE orMissing #-}

derivingUnbox
  "OrMissingInt"
  [t|OrMissing Int -> (Bool, Int)|]
  [e|orMissing (False, 0)(\x -> (True, x))|]
  [e|\(b, x) -> if b then Present x else Missing|]

derivingUnbox
  "OrMissingBool"
  [t|OrMissing Bool -> (Bool, Bool)|]
  [e|orMissing (False, False)(\x -> (True, x))|]
  [e|\(b, x) -> if b then Present x else Missing|]

derivingUnbox
  "OrMissingDouble"
  [t|OrMissing Double -> (Bool, Double)|]
  [e|orMissing (False, 0)(\x -> (True, x))|]
  [e|\(b, x) -> if b then Present x else Missing|]

type instance FStreamly.VectorFor (OrMissing Bool) = UVec.Vector
type instance FStreamly.VectorFor (OrMissing Int) = UVec.Vector
type instance FStreamly.VectorFor (OrMissing Double) = UVec.Vector
type instance FStreamly.VectorFor (OrMissing Text) = Vec.Vector


-- | Derive Unbox instance and the corresponding type family instance for
-- @VectorFor@ and @Unbox@.  Requires @VectorFor@ (from Frames)
-- and @Vector@ (from Data.Vector.Unbox)
-- to be in scope.
-- NB: Importing this module unqualified will put the correct things in scope.
-- NB: This may conflict with other vector imports which also use the @Vector@ name.
-- @Data a@ is required to avoid requiring @Lift a@ which causes stage-restriction issues.
derivingOrMissingUnboxVectorFor :: Text -- ^ Unique constructor suffix for the MVector and Vector data families.  Usually the type name.
                                -> ExpQ -- ^ TH expression for a default value of the type. E.g. @[e|MyType 0|]@
                                -> DecsQ -- ^ declarations for the @Unbox@ and @VectorFor@ instances
derivingOrMissingUnboxVectorFor name defAExpQ = do
  let nameQ = conT (mkName $ T.unpack name)
      typQ = [t|() => OrMissing $(nameQ) -> (Bool, $(nameQ))|]
--      anyAName = mkName "anyA" -- this name must match the argument.
      expNothingQ :: Q Exp = tupE [return $ ConE 'False, defAExpQ]
      srcToRepExpQ = lamE [] $ appE (appE [e|orMissing|] expNothingQ) [e|\x->(True,x)|]
      repToSrcExpQ = [e|\(b, x) -> if b then Present x else Missing|]
  unboxDecs <- derivingUnbox (T.unpack $ "OrMissing" <> name) typQ srcToRepExpQ repToSrcExpQ
  vectorDecs <- [d|type instance VectorFor (OrMissing $(nameQ)) = Vector|]
  return $ unboxDecs <> vectorDecs


-- | Derive @Unbox@ instance and the corresponding type family instance
-- @VectorFor@ for @a@ *and* @OrMissing a@.  Requires @VectorFor@ (from Frames)
-- and @Vector@ (from Data.Vector.Unbox)
-- to be in scope.
-- NB: Importing this module *unqualified* will put the correct things in scope.
-- NB: This may conflict with other vector imports which also use the @Vector@ name.
derivingOrMissingUnboxVectorFor' :: DecsQ
                                 -> Text -- ^ Unique constructor suffix for the MVector and Vector data families.  Usually the type name.
                                 -> ExpQ -- ^ TH expression for a default value of the type. E.g. @[e|EnumA|]@
                                 -> DecsQ -- ^ declarations for the @Unbox@ and @VectorFor@ instances
derivingOrMissingUnboxVectorFor' underlyingUnboxDecsQ name defAExpQ = do
  let nameQ = conT (mkName $ T.unpack name)
  underlyingUnboxDecs <-  underlyingUnboxDecsQ
  underlyingVectorDecs <- [d|type instance VectorFor $(nameQ) = Vector|]
  unboxAndVectorDecs <- derivingOrMissingUnboxVectorFor name defAExpQ
  return $ underlyingUnboxDecs <> underlyingVectorDecs <> unboxAndVectorDecs --unboxDecs <> vectorDecs
