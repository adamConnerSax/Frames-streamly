{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-|
Module      : Frames.Streamly.LoadToCore
Description : Load directly from file to Frame
Copyright   : (c) Adam Conner-Sax 2021
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

-}
module Frames.Streamly.LoadInCore
    (

    )
where

import qualified Frames
import qualified Frames.Streamly.InCore                          as FS
import qualified Frames.Streamly.CSV                          as FS

import Frames.Streamly.Streaming.Class (StreamFunctions(..)) --, FoldType)
{-
loadInCore :: StreamFunctionsIO s m => ParserOptions -> FilePath -> (Frames.Record rs -> Frames.Record rs') -> m (Frames.FrameRec rs')
loadInCore po fp t = sReadTextAndFold fp (sLMapFoldM (t . toRec) inCoreAoS_F) where
  toRec :: Text -> Frames.Rec rs
  toRec =
-}
