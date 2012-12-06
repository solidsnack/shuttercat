{-# LANGUAGE OverloadedStrings #-}

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TVar
import           Control.Concurrent.STM.TChan
import           Control.Exception
import           Control.Monad
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn, pack)
import qualified Data.ByteString.Char8 as ByteString
import           Data.Maybe
import           Data.Monoid
import           Data.Time
import           Data.Time.Clock ()
import           System.Exit
import           System.IO
import           System.Mem

import           Control.Concurrent.STM.ShutterChan


main :: IO ()
main  = go 25000 stdin

go :: Int -> Handle -> IO ()
go t h = do (blocks, chunks) <- atomically ctx
            a <- async $ recv h  blocks
            b <- async $ handoff blocks chunks
            c <- async $ send           chunks
            mapM_ wait [a, b, c]
            exitSuccess
 where ctx     = (,) <$> newTChan <*> newTChan
       handoff = transfer' t now performGC
       now     = (hPutStrLn stderr . show) =<< getCurrentTime

recv :: Handle -> TChan (Maybe ByteString) -> IO ()
recv h o = do bytes <- ByteString.hGetSome h 16384
              ("" /= bytes) `when` atomically (writeTChan o (Just bytes))
              (eof, closed) <- (,) <$> hIsEOF h <*> hIsClosed h
              if eof || closed then atomically (writeTChan o Nothing)
                               else recv h o

send :: TChan [ByteString] -> IO ()
send i = do chunks <- atomically $ readTChan i
            mapM_ ByteString.putStr chunks
            hFlush stdout
            (chunks /= []) `when` send i


msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr

