{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module Frames.Streamly.OrMissing where

import Prelude hiding (Type, lift)
import qualified Data.Text as T
import Data.Vector.Unboxed.Deriving
import Data.Vector.Unboxed (Unbox)
import Language.Haskell.TH
import Language.Haskell.TH.Syntax

data OrMissing a = Missing | Present a deriving (Show, Eq, Ord, Generic, Functor)

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

derivingOrMissingUnbox :: (Lift a, Unbox a)
                     => Text -- ^ the name of type a
                     -> a -- ^ really and truly any value.  Needed for the nothing case; never user visible.
                     -> DecsQ
derivingOrMissingUnbox name anyA = do
  let nameQ = conT (mkName $ T.unpack name)
      typQ = [t|() => OrMissing $(nameQ) -> (Bool, $(nameQ))|]
      expNothing = (False, anyA)
      srcToRepExpQ = [e|orMissing (False, expNothing) (\x -> (True, x))|]
      repToSrcExpQ = [e|\(b, x) -> if b then Present x else Missing|]
  derivingUnbox (T.unpack $ "OrMissing" <> name) typQ srcToRepExpQ repToSrcExpQ
