{-# LANGUAGE Arrows #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import           Prelude hiding (sum)

import           Opaleye (Column, Nullable, matchNullable, isNull,
                         Table(Table), required, optional, queryTable,
                         Query, QueryArr, restrict, (.==), (.<=), (.&&), (.<),
                         (.++), ifThenElse, pgString, aggregate, groupBy,
                         count, avg, sum, leftJoin, runQuery,
                         showSqlForPostgres, Unpackspec,
                         PGInt4, PGInt8, PGText, PGDate, PGFloat8, PGBool)

import           Data.Profunctor.Product (p2, p3)
import           Data.Profunctor.Product.Default (Default, def)
import           Data.Profunctor.Product.TH (makeAdaptorAndInstance)
import           Data.Time.Calendar (Day)

import           Control.Arrow (returnA, (<<<))

import qualified Database.PostgreSQL.Simple as PGS

import qualified Opaleye.Internal.Unpackspec as U

personTable :: Table (Column PGText, Column PGInt4, Column PGText)
                     (Column PGText, Column PGInt4, Column PGText)
personTable = Table "personTable" (p3 ( required "name"
                                      , required "age"
                                      , required "address" ))

personQuery :: Query (Column PGText, Column PGInt4, Column PGText)
personQuery = queryTable personTable

printSql :: Default Unpackspec a a => Query a -> IO ()
printSql = putStrLn . showSqlForPostgres

data Birthday' a b = Birthday { bdName :: a, bdDay :: b }
type Birthday = Birthday' String Day
type BirthdayColumn = Birthday' (Column PGText) (Column PGDate)

$(makeAdaptorAndInstance "pBirthday" ''Birthday')

birthdayTable :: Table BirthdayColumn BirthdayColumn
birthdayTable = Table "birthdayTable"
                      (pBirthday Birthday { bdName = required "name"
                                          , bdDay  = required "birthday" })
birthdayQuery :: Query BirthdayColumn
birthdayQuery = queryTable birthdayTable

-- projection
nameAge :: Query (Column PGText, Column PGInt4)
nameAge = proc () -> do
    (name, age, _) <- personQuery -< ()
    returnA -< (name, age)

-- product
personBirthdayProduct :: Query ((Column PGText, Column PGInt4, Column PGText), BirthdayColumn)
personBirthdayProduct = proc () -> do
    personRow   <- personQuery -< ()
    birthdayRow <- birthdayQuery -< ()

    returnA -< (personRow, birthdayRow)

-- restriction
youngPeople :: Query (Column PGText, Column PGInt4, Column PGText)
youngPeople = proc () -> do
    row@(_, age, _) <- personQuery -< ()
    restrict -< age .<= 18

    returnA -< row

twentiesAtAddress :: Query (Column PGText, Column PGInt4, Column PGText)
twentiesAtAddress = proc () -> do
    row@(_, age, address) <- personQuery -< ()

    restrict -< (20 .<= age) .&& (age .< 30)
    restrict -< address .== pgString "1 My Street, My Town"

    returnA -< row

personAndBirthday :: Query (Column PGText, Column PGInt4, Column PGText, Column PGDate)
personAndBirthday = proc () -> do
    (name, age, address) <- personQuery -< ()
    birthday             <- birthdayQuery -< ()

    restrict -< name .== bdName birthday

    returnA -< (name, age, address, bdDay birthday)

employeeTable :: Table (Column PGText, Column (Nullable PGText))
                       (Column PGText, Column (Nullable PGText))
employeeTable = Table "employeeTable" (p2 ( required "name"
                                          , required "boss" ))
    

hasBoss :: Query (Column PGText)
hasBoss = proc () -> do
    (name, nullableBoss) <- queryTable employeeTable -< ()

    let aOrNo = ifThenElse (isNull nullableBoss) (pgString "no") (pgString "a")
    returnA -< name .++ pgString " has " .++ aOrNo .++ pgString " boss"
    
bossQuery :: QueryArr (Column PGText, Column (Nullable PGText)) (Column PGText)
bossQuery = proc (name, nullableBoss) ->
    returnA -< matchNullable (name .++ pgString " has no boss")
                             (\boss -> pgString "The boss of " .++ name .++ pgString " is " .++ boss)
                             nullableBoss

restrictIsTwenties :: QueryArr (Column PGInt4) ()
restrictIsTwenties = proc age ->
    restrict -< (20 .<= age) .&& (age .< 30)

restrictAddressIs1MyStreet :: QueryArr (Column PGText) ()
restrictAddressIs1MyStreet = proc address ->
    restrict -< address .== pgString "1 My Street, My Town"

twentiesAtAddress' :: Query (Column PGText, Column PGInt4, Column PGText)
twentiesAtAddress' = proc () -> do
    row@(_, age, address) <- personQuery -< ()

    restrictIsTwenties -< age
    restrictAddressIs1MyStreet -< address

    returnA -< row

main :: IO ()
-- main = printSql personQuery >> printSql birthdayQuery
-- main = printSql nameAge
main = printSql personBirthdayProduct


