{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
module Frames.Streamly.Internal.Streaming where

type family FoldType (s :: (Type -> Type) -> Type -> Type) :: (Type -> Type) -> Type -> Type -> Type

data StreamFunctions (s :: (Type -> Type) -> Type -> Type) (m :: Type -> Type) = StreamFunctions
  { sThrowIfEmpty :: forall x. s m x -> m ()
    -- ^ throw an exception if the stream is empty
  , sCons :: forall a. a -> s m a -> s m a
  -- ^ add an element to the head of a stream
  , sUncons :: forall a . s m a -> m (Maybe (a, s m a))
    -- ^ split a stream into it's head and tail, returning @m Nothing@ if the stream was empty
  , sHead :: forall a . s m a -> m (Maybe a)
    -- ^ return the first item of a (possibly empty) stream. @m Nothing@ if the stream was empty
  , sMap :: forall x y. (x -> y) -> s m x -> s m y
    -- ^ map each element of the stream using the given function
  , sMapMaybe :: forall x y. (x -> Maybe y) -> s m x -> s m y
    -- ^ map each element of the stream using the given function
  , sDrop :: forall a.Int -> s m a -> s m a
    -- ^ drop n items from the head of the stream
  , sTake :: forall a.Int -> s m a -> s m a
  -- ^ take first n elemens of the stream and drop the rest
  , sFromEffect :: forall a.m a -> s m a
    -- ^ lift a monadic action returning a into a stream
  , sFolder :: forall x b. (x -> b -> x) -> x -> s m b -> m x
    -- ^ fold the stream using the given step function and starting value
  , sBuildFold :: forall x a b.(x -> a -> x) -> x -> (x -> b) -> FoldType s m a b
    -- ^ Build a fold from (pure) step, start and extract functions
  , sBuildFoldM :: forall x a b.(x -> a -> m x) -> m x -> (x -> m b) -> FoldType s m a b
    -- ^ Build a fold from (monadic) step, start and extract functions
  , sFold :: forall a b.FoldType s m a b -> s m a -> m b
  -- ^ run a fold on a stream
  , sToList :: forall x. s m x -> m [x]
  -- ^ stream to (lazy) list
  , sFromFoldable :: forall f a.Foldable f => f a -> s m a
    -- ^ build a stream of @a@ from a foldable of @a@
  }

data StreamFunctionsIO (s :: (Type -> Type) -> Type -> Type) (m :: Type -> Type) = StreamFunctionsIO
  {
    sReadTextLines :: FilePath -> s m Text
    -- ^ create a stream of lines of text by reading the given file
  , sWriteTextLines :: FilePath -> s m Text -> m ()
    -- ^ streamly version handles invalid characters
  }

data StreamFunctionsWithIO (s :: (Type -> Type) -> Type -> Type) (m :: Type -> Type) = StreamFunctionsWithIO
  {
    streamFunctions :: StreamFunctions s m
  , streamFunctionsIO :: StreamFunctionsIO s m
  }