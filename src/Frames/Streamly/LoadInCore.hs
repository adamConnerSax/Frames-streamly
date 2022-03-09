{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-|
Module      : Frames.Streamly.LoadInCore
Description : Load directly from file to Frame
Copyright   : (c) Adam Conner-Sax 2021
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

-}
module Frames.Streamly.LoadInCore
    (
      loadInCore
--    , loadInCore2
    )
where

import qualified Frames
import qualified Data.Vinyl as V
import qualified Frames.Streamly.InCore                          as FS
import qualified Frames.Streamly.CSV                          as FS
--import qualified Data.Strict.Maybe as Strict.Maybe
import Frames.Streamly.Streaming.Class (StreamFunctions(..), StreamFunctionsIO(..))

loadInCore :: forall s m rs rs'.(StreamFunctionsIO s m, V.RMap rs, FS.StrictReadRec rs, FS.RecVec rs, FS.RecVec rs')
           => FS.ParserOptions
           -> FilePath
           -> (Frames.Record rs -> Maybe (Frames.Record rs'))
           -> (IOSafe s m) (Frames.FrameRec rs')
loadInCore po fp t = FS.inCoreAoS $ sMapMaybe t $ FS.readTableOpt @rs @s @m po fp
{-# INLINEABLE loadInCore #-}

{-
loadInCore2 :: forall s m rs rs'.(StreamFunctionsIO s m, V.RMap rs, FS.StrictReadRec rs, FS.RecVec rs, FS.RecVec rs')
           => FS.ParserOptions -> FilePath -> (Frames.Record rs -> Maybe (Frames.Record rs')) -> (IOSafe s m) (Frames.FrameRec rs')
loadInCore2 po fp t = sReadScanMAndFold @s @m fp (FS.parsingScanF po $ FS.parseOne po) (return FS.AccInitial) fld where
  fromScan :: FS.Acc (Frames.Record rs) -> Maybe (Frames.Record rs')
  fromScan x = FS.accToMaybe x >>= t
  {-# INLINE fromScan #-}
  fld :: FoldType s (IOSafe s m) (FS.Acc (Frames.Record rs)) (Frames.FrameRec rs')
  fld = sLMapFoldM @s (return . fromScan) $ sFoldMaybe @s (FS.inCoreAoS_F @_ @s @(IOSafe s m))
  {-# INLINE fld #-}
{-# INLINE loadInCore2 #-}
-}
