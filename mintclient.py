#! /usr/bin/python3
# @rob4lderman
# ThinMint utils. 
#
# Downloads trans from mint, merges with thinmint db, sends summary email.
#
# Usage: see thinmint.cron.sh
# 
# mintapi APIs:
# mint.get_accounts()
# mint.get_budgets()
# mint.get_transactions()
# mint.get_transactions_csv()
# mint.get_transactions_json()
# mint.get_net_worth()  // just adds up the values in get_accounts
# mint.initiate_account_refresh()
# 

import json
import mintapi1
import getopt
import sys
import re
import functools

#
# API: https://www.dlitz.net/software/pycrypto/api/current/
#
from Crypto.Cipher import AES
from Crypto import Random

import base64
import hashlib
import os

from datetime import date, datetime, timedelta

from mailer import Mailer
from mailer import Message

from pymongo import MongoClient

from postmark import PMMail

import locale
locale.setlocale( locale.LC_ALL, 'en_US.utf8' )



#
# Parse command line args
#
# @return a dictionary of opt=val
#
def parseArgs():
    long_options = [ "mintuser=", 
                     "mintpass=", 
                     "action=", 
                     "startdate=", 
                     "inputfile=", 
                     "outputfile=", 
                     "mintfile=",
                     "transfile=",
                     "accountsfile=",
                     "gmailuser=",
                     "gmailpass=",
                     "to=",
                     "mongouri=",
                     "daysago=",
                     "user=",
                     "pass="]
    opts, args = getopt.getopt( sys.argv[1:], "", long_options )
    retMe = {}
    for opt,val in opts:
       retMe[ opt ] = val
    return retMe

#
# Verify the args map contains the given required_args
#
# @return args
#
# @throws RuntimeError if required_arg is missing.
#
def verifyArgs( args, required_args ):
    for arg in required_args:
        if arg not in args:
            raise RuntimeError( 'Argument %r is required' % arg )
        elif len(args[arg]) == 0:
            raise RuntimeError( 'Argument %r is required' % arg )
    return args


#
# Remove datetime fields so we can format the json
#
# @return accounts
#
def removeDatetime( accounts ):
    print( "removeDatetime: Removing datetime fields...")
    DATE_FIELDS = [
        'addAccountDate',
        'closeDate',
        'fiLastUpdated',
        'lastUpdated' ]
    for account in accounts:
        for df in DATE_FIELDS:
            if df + "InDate" in account:
                del account[df + 'InDate']
    return accounts


#
# @return account data in thinmint format
#
def convertAccount( account ):
    account["_id"] = account["id"]
    account["mintMarker"] = 1


#
# @return accounts data in thinmint format
#
def convertAccounts( accounts ):
    accounts = removeDatetime(accounts)
    for account in accounts:
        convertAccount( account )
    return accounts


#
# send account refresh signal to mint.
#
def refreshMintAccounts( args ):
    print( "refreshMintAccounts: Logging into mint..." )
    mint = mintapi1.Mint( args["--mintuser"], args["--mintpass"] )
    print( "refreshMintAccounts: Refreshing accounts..." )
    mint.initiate_account_refresh()


#
# Retrieve account data from mint
# @return accounts object
#
def getMintAccounts( args ):
    print( "getMintAccounts: Logging into mint..." )
    mint = mintapi1.Mint( args["--mintuser"], args["--mintpass"] )

    print( "getMintAccounts: Getting accounts..." )
    accounts = mint.get_accounts(True)  # True - get extended account detail (takes longer)
    return accounts


#
# --action getMintAccounts
# 
def doGetMintAccounts( args ):
    accounts = convertAccounts( getMintAccounts(args) )
    writeJson( accounts, args["--outputfile"] )


#
# Retrieve transaction data from mint
# @return transactions object
# 
def getMintTransactions( args ):
    print( "getMintTransactions: Logging into mint..." )
    mint = mintapi1.Mint( args["--mintuser"], args["--mintpass"] )

    print( "getMintTransactions: Getting transactions..." )
    trxs = mint.get_transactions_json(include_investment=True, skip_duplicates=False )     # TODO: start_date
    print( "getMintTransactions: Transactions gotten: ", len(trxs) )
    return trxs


#
# Write the given json object to the given file
#
def writeJson( jsonObj, filename ):
    print( "writeJson: writing to file " + filename + "...");
    f = open(filename, "w")
    f.write( json.dumps( jsonObj, indent=2, sort_keys=True) )
    f.close()


#
# Write the given array of lines to the given file
#
def writeLines( lines, filename ):
    print( "writeLines: writing to file " + filename + "...");
    f = open(filename, "w")
    for line in lines:
        f.write( line + "\n")
    f.close()

#
# @return lines[] from the given file
#
def readLines( filename ):
    print( "readLines: reading from file " + filename + "...");
    f = open(filename, "r")
    lines = f.readlines()
    f.close()
    return lines


#
# --action getMintTransactions
# 
def doGetMintTransactions( args ):
    trxs = convertTransactions( getMintTransactions(args) )
    writeJson( trxs, args["--outputfile"] )  


#
# @return a jsonobj as read from the given json file
#
def readJson( filename ):
    print( "readJson: reading from file " + filename + "...");
    f = open(filename, "r")
    jsonObj = json.load(f)
    f.close()
    return jsonObj


#
# --action readTransactions
#
def doReadTransactions( args ):
    trxs = readJson( args["--inputfile"] )  
    print("doReadTransactions: read transactions: ", len(trxs))


#
# --action readAccounts
#
def doReadAccounts( args ):
    accounts = readJson( args["--inputfile"] )
    print("doReadAccounts: read accounts: ", len(accounts))


#
# Convert date strings from "Jan 30" to "01/30/16".
#
# @param datestr the date string to convert
# @param regex = re.compile( r'\d\d/\d\d/\d\d' )
#
# @return converted date string
#
def convertDate( datestr, regex = re.compile( r'\d\d/\d\d/\d\d' ) ):
    if regex.match( datestr ):
        return datestr
    else:
        # must be in the form "Jan 30" or "Feb 1" or whatever
        datestr = datestr + " {}".format( date.today().year )
        tmpdate = datetime.strptime(datestr, "%b %d %Y")
        return tmpdate.strftime("%m/%d/%y")


#
# @param datestr in the format mm/dd/yy
# 
# @return timestamp (seconds since epoch)
#
def getTimestamp( datestr ): 
    return int( datetime.strptime( datestr, "%m/%d/%y").timestamp() )


#
# @return the trx object converted from mint style to thinmint style.
# 
def convertTransaction( trx, regex = re.compile( r'\d\d/\d\d/\d\d' )):
    trx["date"] = convertDate( trx["date"], regex )
    trx["timestamp"] = getTimestamp( trx["date"] )
    trx["_id"] = trx["id"]
    trx["amountValue"] = getSignedTranAmount( trx )
    trx["mintMarker"] = 1
    return trx


#
# @return thinmint formatted trxs. trxs are modified in place.
#
def convertTransactions( trxs ):
    regex = re.compile( r'\d\d/\d\d/\d\d' )     # for date conversions
    for trx in trxs:
        convertTransaction( trx, regex )
    return trxs

#
# Convert trxs from an array to map, indexed by "id"
#
# @return thinmint trxs
#
def convertTransactionsToMap( trxs ):
    retMe = {}
    for trx in trxs:
        retMe[ str(trx["id"]) ] = trx
    return retMe


#
# --action convertTransactionsToMap
#
def doConvertTransactionsToMap( args ):
    trxs = readJson( args["--inputfile"] )  
    print("doConvertTransactions: read transactions: ", len(trxs))
    trxs = convertTransactionsToMap( trxs )
    writeJson( trxs, args["--outputfile"] )


#
# Add all trxs in mintTrxs to thinmintTrxs if it doesn't already exist.
#
# @param mintTrxs array of trxs data downloaded from mint (doGetMintTransactions)
# @param thinmintTrxs map of trxs data
#
# @return thinmintTrxs
#
def mergeTransactions( thinmintTrxs, mintTrxs ):
    for trx in mintTrxs:
        if str(trx["id"]) not in thinmintTrxs:
            print("mergeTransactions: merging transaction id ", trx["id"])
            thinmintTrxs[ str(trx["id"]) ] = convertTransaction(trx)
    return thinmintTrxs


#
# --action mergeTransactions
#
# Read newly read mint txns from --mintfile.
# Read existing thinmint txn db from --inputfile
# Merge new mint txns into thinmint txns.
# Write merged thinmint txns to --outputfile
#
def doMergeTransactions( args ):
    thinmintTrxs = readJson( args["--inputfile"] )  
    mintTrxs = readJson( args["--mintfile"] )  
    thinmintTrxs = mergeTransactions( thinmintTrxs, mintTrxs )
    writeJson( thinmintTrxs, args["--outputfile"] )


#
# Run the given function for each trx in trxs.
#
# @param trxs map of trxId -> trx
# @func function to apply
# 
def forEachTransactionMap( trxs, func ):
    for trxId in trxs:
        trxs[ trxId ] = func( trxs[ trxId ] )
    return trxs


#
# @return the filtered map of trxs
#
def filterTransactions( trxs, func ):
    retMe = {}
    for trxId in trxs:
        if func( trxs[ trxId ] ) == True:
            retMe[ trxId ] = trxs[ trxId ]
    return retMe


#
# Set the hasBeenAcked field
#
def setHasBeenAcked( trx ):
    trx[ "hasBeenAcked" ] = True
    return trx


#
# @return a subset of fields, typically for printing.
#
def pruneTran( tran ):
    if (tran is not None):
        return {k: tran.get(k) for k in ('id', 'account', 'amount', 'fi', 'date', 'timestamp', 'isPending', 'hasBeenAcked', 'isDebit', 'merchant', 'tags', 'mintMarker')}
    else:
        return None

#
# @return a subset of fields, typically for printing.
#
def pruneAccount( account ):
    return {k: account.get(k) for k in ('accountId', 'accountName', 'fiName', 'accountType', 'currentBalance', 'value', 'isActive', 'lastUpdated', 'lastUpdatedInString', 'mintMarker')}


#
# @return formatted trx summary in plain text
#
def formatNewTranText( trx ):
    return "{} {} {} [{}] {}, tags={}".format( trx["date"], 
                                    ("" if trx["isDebit"] else "+") + trx["amount"],
                                    trx["merchant"],
                                    trx["fi"] + ": " + trx["account"], 
                                    "(pending)" if trx["isPending"] else "",
                                    trx.get("tags", []) )

#
# @return formatted trx summary in html
#
def formatNewTranHtml( trx ):
    return "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format( trx["date"], 
                                                                                      ("" if trx["isDebit"] else "+") + trx["amount"],
                                                                                      trx["merchant"],
                                                                                      trx["fi"] + ": " + trx["account"], 
                                                                                      "(pending)" if trx["isPending"] else "" )

#
# @return lines[] of formatted trx data 
#
def formatNewTrans( newTrxs, formatFunc ):
    retMe = []
    for trx in newTrxs:
        retMe.append( formatFunc( trx ) )
    return retMe



#
# @return formatted account summary
#
def formatAccountText( act ):
    return "{} {}".format( locale.currency( act["currentBalance"] ),
                           act["fiName"] + ": " + act["accountName"])

#
# @return formatted account summary
#
def formatAccountHtml( act ):
    return "<tr><td>{}</td><td>{}</td></tr>".format( locale.currency( act["currentBalance"] ),
                                                     act["fiName"] + ": " + act["accountName"])
                                                    


#
# @return lines[] of formatted account data
#
def formatAccounts( accounts, formatFunc ):
    retMe = []
    for act in accounts:
        retMe.append( formatFunc( act ) )
    return retMe


#
# @return a plain text version of the summary email
# 
def composeTextEmail( accounts, newTrxs ):
    retMe = []
    retMe.append( "www.mintwrap.com: Summary of new transactions:")
    retMe.append("")
    retMe += formatNewTrans( newTrxs, formatNewTranText )
    retMe.append("")
    retMe.append("")
    retMe.append( "Summary of accounts:" )
    retMe.append("")
    retMe += formatAccounts( accounts, formatAccountText)
    return retMe


#
# @return an html version of the summary email
# 
def composeHtmlEmail( accounts, newTrxs ):
    retMe = []
    retMe.append("<b><a href='www.mintwrap.com'>Login to ThinMint</a>: Summary of new transactions</b><br/>")
    retMe.append("<table>")
    retMe += formatNewTrans( newTrxs, formatNewTranHtml )
    retMe.append("</table>")
    retMe.append("<br/>")
    retMe.append("<br/>")
    retMe.append("<b>Summary of accounts</b><br/>" )
    retMe.append("<table>")
    retMe += formatAccounts( accounts, formatAccountHtml)
    retMe.append("</table>")
    return retMe

#
# @return trans that have yet to be acked.
#
def getNonAckedTransactions( db, sort ):
    trans = db.transactions.find( { 
                                      "$or": [ { "hasBeenAcked": { "$exists": False } }, { "hasBeenAcked" : False } ],
                                      "mintMarker": 1
                                  },
                                  sort=sort )
    print("getNonAckedTransactions: Found {} non-ACKed transactions out of {}".format( trans.count(), db.transactions.count() ) )
    return trans

#
# @return active bank and credit card accounds
#
def getActiveBankAndCreditAccounts( db ):
    accounts = db.accounts.find( { "$and": [ { "accountType": { "$in": [ "credit", "bank" ] } }, { "isActive" : True } ] } )
    print("getActiveBankAndCreditAccounts: Found {} active bank/credit accounts out of {}".format( accounts.count(), db.accounts.count() ) )
    return accounts

#
# @return active accounts
#
def getActiveAccounts( db ):
    accounts = db.accounts.find( { "isActive" : True,
                                   "mintMarker": 1 } )
    print("getActiveAccounts: Found {} active bank/credit accounts out of {}".format( accounts.count(), db.accounts.count() ) )
    return accounts


#
# Compose an email summary and write it to the --outputfile
#
def doComposeEmailSummary( args ) : 

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    # read trans that have yet to be acked.
    trans = getNonAckedTransactions( db, sort=[ ("timestamp", -1) ] )

    # read active bank and credit card accounts
    accounts = getActiveBankAndCreditAccounts( db )
    
    writeLines( composeHtmlEmail( accounts, trans ), args["--outputfile"] + ".html" )

    # need to rewind the cursors
    trans.rewind()
    accounts.rewind()
    writeLines( composeTextEmail( accounts, trans ), args["--outputfile"] )


#
# Compose and send an email summarizing new un-acked trans
#
def composeAndSendEmailSummary( args ) : 

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    # read trans that have yet to be acked.
    trans = list( getNonAckedTransactions( db, sort=[ ("timestamp", -1) ] ) )

    print("composeAndSendEmailSummary: len(trans):", len(trans))

    # don't bother sending the email if there are few new trans
    if len(trans) < 7:
        return

    # read active bank and credit card accounts
    accounts = list(getActiveBankAndCreditAccounts( db ))
    
    html_body = "".join( composeHtmlEmail( accounts, trans ) )
    text_body = "\n".join( composeTextEmail( accounts, trans ) )

    sendEmail( "team@surfapi.com", 
               args["--user"], 
               "ThinMint: You have {0} un-ACK-nowledged transactions".format( len(trans) ),
               text_body,
               html_body )


#
# Compose an email summary and write it to the --outputfile
# NOTE: this won't work anymore since i changed formatNewTrxs to take an iterable instead of a map..
#
def doComposeEmailSummary_OLD( args ) : 
    trxs = readJson( args[ "--transfile" ] )
    accounts = readJson( args[ "--accountsfile" ] )
    newTrxs = filterTransactions( trxs, lambda trx: "hasBeenAcked" not in trx or trx["hasBeenAcked"] == False )

    # filter for active bank and credit accounts.
    # note: filter() returns an iterable (for lazy filtering), not a list. need to convert to a list,
    # otherwise we won't be able to iterate the iterable
    accounts = list(filter(lambda act: act["isActive"] and act["accountType"] in [ "credit", "bank" ], accounts ))
    
    writeLines( composeHtmlEmail( accounts, newTrxs ), args["--outputfile"] + ".html" )
    writeLines( composeTextEmail( accounts, newTrxs ), args["--outputfile"] )


#
# Send the email.
# Note: need to enable "less secure apps" in gmail: https://www.google.com/settings/security/lesssecureapps
#
def sendEmailSummary( args ):

    message = Message(From=args["--gmailuser"],
                      To=args["--to"])

    message.Subject = "ThinMint Daily Account Summary"

    emailLinesText = readLines( args["--inputfile"] )
    emailLinesHtml = readLines( args["--inputfile"] + ".html" )

    message.Html = "".join( emailLinesHtml )
    message.Body = "\n".join( emailLinesText )

    sender = Mailer('smtp.gmail.com', 
                     port=587,
                     use_tls=True, 
                     usr=args["--gmailuser"],
                     pwd=args["--gmailpass"] )
    sender.send(message)


#
# Send email
#
def sendEmail( fromAddr, toAddr, subject, text_body, html_body):

    print("sendEmail: fromAddr:", fromAddr,
                       "toAddr:", toAddr,
                      "subject:", subject,
                    "text_body:", text_body,
                    "html_body:", html_body )

    message = PMMail(api_key = os.environ.get('POSTMARK_API_TOKEN'),
                     subject = subject,
                     sender = fromAddr,
                     to = toAddr,
                     text_body = text_body,
                     html_body = html_body,
                     tag = "tm") 
    message.send()


#
# @return a ref to the mongo db by the given name.
#
def getMongoDb( mongoUri ):
    dbname = mongoUri.split("/")[-1]
    hostname = mongoUri.split("@")[-1]
    mongoClient = MongoClient( mongoUri )
    print("getMongoDb: connected to mongodb://{}, database {}".format( hostname, dbname ) )
    return mongoClient[dbname]


#
# My first Python class!
# 
class UserDb:

    #
    # CTOR
    #
    def __init__( self, mongoDb, userId ):
        self.userId = userId
        self.db = mongoDb

        for collectionName in ["tags", "accounts", "accountsTimeSeries", "transactions", "savedqueries", "tagsByMonth" ]:
            setattr(self, collectionName, self.getUserCollection(mongoDb, userId, collectionName))

    #
    # @return The user-specific collection
    #
    def getUserCollection(self, db, userId, collectionName):
        return db["/tm/" + userId + "/" + collectionName]


#
# @return userDb object, where collections are routed to the user-specific collection
#
def getUserDb( db, userId ):
    return UserDb(db, userId)

#
# verify getUserDb works.
# 
def checkUserDb(args):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )
    print("checkUserDb db: ", db)
    print("checkUserDb db.accounts: ", db.accounts)
    print("checkUserDb db.accountsTimeSeries: ", db.accountsTimeSeries)


#
# Upsert account record.
#
def upsertAccount( db, account ):
    print("upsertAccount: account=", pruneAccount(account))
    db.accounts.update_one( { "_id": account["_id"] }, 
                                { "$set": account }, 
                                upsert=True )


#
# Insert or update mint account records into mongo.
#
def upsertAccounts( db, accounts ):
    for account in accounts:
        upsertAccount(db, account)
    print("upsertAccounts: db.accounts.count()=", db.accounts.count())


#
# @return the datestr part of {accountId}.{datestr}
#
def parseDateFromAccountsTimeSeriesId( record ):
    retMe = record["_id"].split(".")[1]
    print("parseDateFromAccountsTimeSeriesId: retMe=" + retMe, ", _id=", record["_id"] )
    return retMe

#
# @return str(account["_id"]) + datestr
# 
def getAccountTimeSeriesId( account, datestr ):
    return getAccountIdTimeSeriesId( account["_id"], datestr)

#
# @return str(account["_id"]) + datestr
# 
def getAccountIdTimeSeriesId( accountId, datestr ):
    return str(accountId) + "." + datestr


#
# Looks like only fiLastUpdated field has timestamp in ms.
# All other timestamps in seconds.
#
# @param timestamp_ms in milliseconds
#
# @return datestr mm/dd/yy for the given timestamp_ms
#
def formatDateString_ms( timestamp_ms ):
    return formatDateString_s( timestamp_ms / 1000 )


#
# @param timestamp_s in seconds
#
# @return datestr mm/dd/yy for the given timestamp_s
#
def formatDateString_s( timestamp_s ):
    return datetime.fromtimestamp( timestamp_s ).strftime( "%m/%d/%y" )


#
# @return relevant time-series data from the given account (e.g. balance)
# 
def getAccountTimeSeriesData( account, datestr ):
    retMe = {k: account[k] for k in ('accountId', 'accountName', 'currentBalance', 'value')}
    retMe["date"] = datestr
    retMe["timestamp"] = getTimestamp(datestr)
    return retMe


#
# @return an accountTimeSeries record for the given account and date
# 
def createAccountTimeSeriesRecord( account ):
    datestr = formatDateString_ms( account["fiLastUpdated"] ) 
    retMe = getAccountTimeSeriesData( account, datestr )
    retMe["_id"] = getAccountTimeSeriesId( account, datestr ) 
    print( "createAccountTimeSeriesRecord: ", retMe )
    return retMe

#
# @param accountId
# @param accountName
# @param beforeDate the date for the record
# @param value net worth on this date
#
# @return an accountTimeSeries record for the given data
# 
def createSummaryTimeSeriesRecord( accountId, accountName, beforeDate, value ):
    datestr = beforeDate.strftime( "%m/%d/%y" )

    retMe = {}
    retMe["accountId"] = accountId
    retMe["accountName"] = accountName
    retMe["date"] = datestr
    retMe["timestamp"] = int( beforeDate.timestamp() )
    retMe["value"] = value 
    retMe["currentBalance"] = value
    retMe["_id"] = getAccountIdTimeSeriesId( accountId, datestr ) 

    print( "createSummaryTimeSeriesRecord: ", retMe )
    return retMe

#
# @param accountId
# @param accountName
# @param beforeDate the date for the record
# @param value net worth on this date
#
# @return an accountTimeSeries record for the given data
# 
def createBackfillSummaryTimeSeriesRecord( accountId, accountName, beforeDate, value ):
    retMe = createSummaryTimeSeriesRecord(accountId, accountName, beforeDate, value)
    retMe["isBackfill"] = True
    print( "createBackfillSummaryTimeSeriesRecord: ", retMe )
    return retMe


#
# @param beforeDate the date for the record
# @param netWorth net worth on this date
#
# @return an accountTimeSeries record for the given data
# 
def createNetWorthTimeSeriesRecord( beforeDate, value ):
    return createSummaryTimeSeriesRecord( -1, "Net Worth", beforeDate, value)


#
# @param beforeDate the date for the record
# @param value the value on this date
#
# @return an accountTimeSeries record for the given data
# 
def createBankAndCreditTimeSeriesRecord( beforeDate, value ):
    return createSummaryTimeSeriesRecord( -2, "Bank & Credit Accounts", beforeDate, value)


#
# @param beforeDate the date for the record
# @param value the value on this date
#
# @return an accountTimeSeries record for the given data
# 
def createNonBankAndCreditTimeSeriesRecord( beforeDate, value ):
    return createSummaryTimeSeriesRecord( -3, "Investment & Other Accounts", beforeDate, value)


#
# @param account
# @param timestamp the new record's timestamp
# @param nextRecord the next/subsequent/later time-series record for this account
# @param tranSum the sum of tran amounts in the intervening time between timestamp and nextRecord.timestamp
#
# @return a "backfill" accountTimeSeries record for the given data
# 
def createBackfillAccountTimeSeriesRecord( account, timestamp, nextRecord, tranSum ):
    datestr = formatDateString_s( timestamp )
    retMe = getAccountTimeSeriesData( account, datestr )

    # subtract the tran amount, since we're going backwards in time (like we're rolling back the trans)
    retMe["value"] = nextRecord["value"] - tranSum
    retMe["currentBalance"] = -1 * retMe["value"] if account["accountType"] == "credit" else retMe["value"]

    retMe["_id"] = getAccountTimeSeriesId( account, datestr ) 
    retMe["isBackfill"] = True

    print( "createBackfillAccountTimeSeriesRecord: ", retMe )
    return retMe


#
# Insert or update a single account time-series record into mongo.
# Time-series records keep track of account balance from day-to-day.
#
def upsertAccountsTimeSeriesRecord( db, record ):
    db.accountsTimeSeries.update_one( { "_id": record["_id"] }, 
                                      { "$set": record }, 
                                      upsert=True )

#
# @return the sum of all account values.
# 
def sumAccountValues(accounts):
    retMe = functools.reduce( lambda memo, account: memo + ( account["value"] if account is not None else 0 ),
                              accounts,
                              0 )
    print("sumAccountValues: ", retMe )
    return retMe


#
# Insert or update account time-series records into mongo.
# Time-series records keep track of account balance from day-to-day.
#
def upsertSummaryTimeSeries( db, accounts ):

    # Net worth
    netWorth = sumAccountValues( accounts )
    netWorthRecord = createNetWorthTimeSeriesRecord( datetime.today(), netWorth )
    upsertAccountsTimeSeriesRecord( db, netWorthRecord )

    # Bank/Credit accounts
    bankAndCreditValue = sumAccountValues( filterBankAndCreditAccounts(accounts) )
    bankAndCreditRecord = createBankAndCreditTimeSeriesRecord( datetime.today(), bankAndCreditValue )
    upsertAccountsTimeSeriesRecord( db, bankAndCreditRecord)

    # Investment/other accounts
    nonBankAndCreditValue = sumAccountValues( filterNonBankAndCreditAccounts(accounts) )
    nonBankAndCreditRecord = createNonBankAndCreditTimeSeriesRecord( datetime.today(), nonBankAndCreditValue )
    upsertAccountsTimeSeriesRecord( db, nonBankAndCreditRecord)

    print("upsertSummaryTimeSeries: db.accountsTimeSeries.count()=", db.accountsTimeSeries.count())

#
# Insert or update account time-series records into mongo.
# Time-series records keep track of account balance from day-to-day.
#
def doUpsertSummaryTimeSeries( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )
    upsertSummaryTimeSeries( db, list(getActiveAccounts(db)) )


#
# Insert or update account time-series records into mongo.
# Time-series records keep track of account balance from day-to-day.
#
def upsertAccountsTimeSeries( db, accounts ):
    for account in accounts:
        record = createAccountTimeSeriesRecord( account )
        upsertAccountsTimeSeriesRecord( db, record )

    upsertSummaryTimeSeries(db, accounts)

    print("upsertAccountsTimeSeries: db.accountsTimeSeries.count()=", db.accountsTimeSeries.count())


#
# Insert or update mint transaction record into mongo.
#
def upsertTransaction(tran, db):
    print("upsertTransaction: tran=", pruneTran(tran))
    db.transactions.update_one( { "_id": tran["_id"] }, 
                                { "$set": tran }, 
                                upsert=True )


#
# Insert or update mint transaction records into mongo.
#
def upsertTransactions( db, trans ):
    print("upsertTransactions: len(trans)", len(trans))
    for tran in trans:
        upsertTransaction(tran, db)
    print("upsertTransactions: db.transactions.count()=", db.transactions.count())


#
# @return subset of accounts with isActive=True
#
def filterActiveAccounts( accounts ):
    return list( filter(lambda act: act["isActive"], accounts) ) 


#
# @return subset of accounts of type "credit" or "bank"
#
def filterBankAndCreditAccounts( accounts ):
    return list( filter(lambda account: account["accountType"] in [ "credit", "bank" ], accounts) ) 


#
# @return subset of accounts NOT of type "credit" or "bank"
#
def filterNonBankAndCreditAccounts( accounts ):
    return list( filter(lambda account: account["accountType"] not in [ "credit", "bank" ], accounts) ) 


#
# Set the given user at args["--user"]
# @return args
#
def setUser(args, user):
    args["--user"] = user
    return args

#
# @return the '--user' field from the given args
# 
def getUser(args):
    return args["--user"]

#
# @return args
#
def setMongoUri(args):
    args['--mongouri'] = decryptCreds( os.environ["TM_MONGO_URI"] )
    return args

#
# Download all accounts and trans from mint.
# Push mint accounts into db.accounts
#   Note: existing account data will be overwritten.  This is OK since I don't intend to modify ANY mint data.
#
def importMintDataToMongo( args ):
    # make sure we can reach mongo first
    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    mintAccounts = convertAccounts( getMintAccounts( args ) )
    mintTrans = convertTransactions( getMintTransactions( args ) )

    if ( len(mintAccounts) > 0 ): 
        # clear the mintMarker field, so we can tell which accounts in the thinmint db
        # have been deleted from the mint db.
        db.accounts.update_many( {}, { "$set": { "mintMarker": 0, "isActive": False } } )
        upsertAccounts( db, mintAccounts )
        upsertAccountsTimeSeries( db, filterActiveAccounts( mintAccounts ) )


    if ( len(mintTrans) > 0 ): 
        # clear the mintMarker field, so we can tell which pending trans in the thinmint db
        # have been deleted from the mint db.
        db.transactions.update_many( {}, { "$set": { "mintMarker": 0 } } )
        upsertTransactions( db, mintTrans )


#
# @return true if the two merchant strings match
#
def isMerchantMatch( merchant1, merchant2 ):
    merchant1 = merchant1.lower()
    merchant2 = merchant2.lower()
    if (merchant1 == merchant2):
        return True
    elif ( len(merchant1) <= len(merchant2) ):
        return merchant2.startswith( merchant1 )
    else:
        return merchant1.startswith( merchant2 )


#
# @return true if float1 is within tolerance of float2
#
def isFloatWithin( float1, float2, tolerance ):
    if (float1 == float2):
        retMe = True
    elif (float1 > float2):
        retMe = float1 < (float2 + tolerance)
    elif (float1 < float2):
        retMe = float1 > (float2 - tolerance)
    print("isFloatWithin: ", float1, float2, tolerance, retMe )
    return retMe


#
# @param currencystr e.g. "$5.99"
#
# @return the value as a float
#
def currencyToFloat( currencystr ):
    return locale.atof(currencystr[1:])


#
# @return true if the merchant matches and the amount is nearly a match
#
def isPendingTranCloseMatch( pendingTran, tran ):
    return isMerchantMatch( pendingTran["merchant"], tran["merchant"] ) and isFloatWithin( tran["amountValue"], 
                                                                                           pendingTran["amountValue"],
                                                                                           abs(pendingTran["amountValue"] * 0.30) )
#
# @return true if the merchant and amount matches.
#
def isPendingTranExactMatch( pendingTran, tran ):
    return isMerchantMatch( pendingTran["merchant"], tran["merchant"] ) and (tran["amountValue"] == pendingTran["amountValue"])


#
# Copy the tags from the pendingTran to the clearedTran.
# Set the clearedTran["pendingTran"] field.
#
# @sideeffect: pendingTran and clearedTran are modified
#
def linkPendingTran( pendingTran, clearedTran ):
    
    applyTags( pendingTran, clearedTran )

    clearedTran["pendingTran"] = {k: pendingTran.get(k) for k in ('id', 'date', 'merchant', 'amount', 'amountValue')}

    # -rx- pendingTran['isResolved'] = True  # TODO: don't need this if gonna delete mintMarker=0
    # -rx- pendingTran['hasBeenAcked'] = True

    print("linkPendingTran: pendingTran: ", pruneTran(pendingTran) )
    print("linkPendingTran: clearedTran: " , pruneTran(clearedTran) )


#
# Do a full update of the given tran in the db.
#
def updateTran( tran, db ):
    print("updateTran: tran._id=", tran["_id"])
    db.transactions.update_one( { "_id": tran["_id"] }, 
                                { "$set": tran } )


#
# @return a list of clearedTrans that are likely matches for the given pendingTran
#
def findMatchingClearedTrans( pendingTran, db ):

    trans = db.transactions.find( { "isPending" : False, 
                                    # TODO "hasBeenAcked": { "$exists": False },   # if the cleared tran has already been ack'ed, don't mess with it
                                    "pendingTran": { "$exists": False },    # ignore cleared trans that have already been linked to another pendingtran
                                    "fi": pendingTran["fi"], 
                                    "account": pendingTran["account"],
                                    "mintMarker": 1,
                                    "timestamp": { "$lte": (pendingTran["timestamp"] + (86400 * 10) ),   # cleared within a week or so
                                                   "$gte": pendingTran["timestamp"] 
                                                 }              
                                  },
                                  sort=[ ("timestamp", 1) ] )   # start with the earliest tran and work our way forward

    print("findMatchingClearedTrans: Searching for potential matches for ", pruneTran(pendingTran) , ". Potential match count: ", trans.count());

    # first look for exact matches (merchant and amountValue)
    matches = list( filter( lambda tran: isPendingTranExactMatch(pendingTran, tran), trans ) )

    if ( len(matches) == 0 ): 
        # no exact matches... look for close matches (sometimes the amount changes, e.g. adding a tip)..
        trans.rewind()
        matches = list( filter( lambda tran: isPendingTranCloseMatch(pendingTran, tran), trans ) )

    if ( len(matches) > 0 ): 
        print("findMatchingClearedTrans: ====> match:", pruneTran( matches[0] ) )

    return matches


#
# For all pending trans not yet resolved...
# Look for matches
# Link matches to pendingTran
# Push pendingTran tags to matches
# Mark pendingTran resolved.
#
def resolvePendingTransactions( args ):
    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    pendingTrans = db.transactions.find( { "isPending" : True, 
                                           "mintMarker" : 0
                                           # -rx- "isResolved": { "$exists": False }  # TODO:don't need this if we're gonna delete the resolved tran
                                         },
                                         sort=[ ("timestamp", 1) ] )   # start with the earliest tran and work our way forward

    for pendingTran in pendingTrans:
        clearedTrans = findMatchingClearedTrans( pendingTran, db )
        if clearedTrans:
            linkPendingTran( pendingTran, clearedTrans[0] )
            updateTran(clearedTrans[0], db)   
            # -rx- updateTran(pendingTran, db)


#
# Remove thinmint pending trans that have been removed from mint
# remove pending trans with mintMarker=0, which means that mint has deleted them.
# 
def syncRemovedPendingTrans( args ):
    print("syncRemovedPendingTrans: ")
    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )
    db.transactions.remove(  { "isPending" : True, 
                               "mintMarker" : 0
                             } )


#
# Set timestamp field in all trans that don't have one
#
def setTransactionTimestamps( args ):
    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    trans = db.transactions.find( { "timestamp": { "$exists": False } } )

    print("setTransactionTimestamps: tran count:", trans.count())

    for tran in trans:
        tran["timestamp"] = getTimestamp( tran["date"] )
        updateTran(tran, db)   

#
# Set transaction amountValue
#
def setTransactionAmountValues( args ):
    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    trans = db.transactions.find( { "amountValue": { "$exists": False } } )

    print("setTransactionAmountValues: tran count:", trans.count())

    for tran in trans:
        tran["amountValue"] = getSignedTranAmount( tran )
        updateTran(tran, db)   


#
# Set timestamp field in all accountsTimeSeries that don't have one.
#
def setAccountsTimeSeriesTimestamps( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    records = db.accountsTimeSeries.find( { "timestamp": { "$exists": False } } )

    print("setAccountsTimeSeriesTimestamps: recourd count:", records.count())

    for record in records:
        datestr = parseDateFromAccountsTimeSeriesId( record )
        record["date"] = datestr
        record["timestamp"] = getTimestamp(datestr)
        db.accountsTimeSeries.update_one( { "_id": record["_id"] }, 
                                          { "$set": record } )


#
# @return the first record in accountsTimeSeries for the given account after the given date
#
def getFirstAccountTimeSeriesRecordAfterDate( account, begindate, db ):
    retMe = db.accountsTimeSeries.find_one( { "accountId": account["accountId"],
                                              "timestamp": { "$gte": begindate.timestamp() } },
                                            sort=[ ("timestamp", 1) ] )
    print("getFirstAccountTimeSeriesRecordAfterDate: begindate:", begindate.timestamp(), "retMe:", retMe )
    return retMe 

#
# @return the first record in accountsTimeSeries for the given account BEFORE the given date
#
def getPreviousAccountTimeSeriesRecordBeforeDate( account, begindate, db ):
    retMe = db.accountsTimeSeries.find_one( { "accountId": account["accountId"],
                                              "timestamp": { "$lte": begindate.timestamp() } },
                                            sort=[ ("timestamp", -1) ] )
    print("getPreviousAccountTimeSeriesRecordBeforeDate: begindate:", begindate.timestamp(), "retMe:", retMe )
    return retMe 


#
# @return the last record in accountsTimeSeries for the given account 
#
def getLastAccountTimeSeriesRecord( account, db ):
    retMe = db.accountsTimeSeries.find_one( { "accountId": account["accountId"] },
                                            sort=[ ("timestamp", -1) ] )
    print("getLastAccountTimeSeriesRecord: retMe:", retMe )
    return retMe 


#
# @return the first non-back-filled record (isBackfill=false) in accountsTimeSeries for the given account 
#
def getFirstNonBackfillAccountsTimeSeriesRecord( account, db ):
    retMe = db.accountsTimeSeries.find_one( { "accountId": account["accountId"],
                                              "isBackfill": { "$exists": False } },
                                             sort=[ ("timestamp", 1) ] )
    print("getFirstNonBackfillAccountsTimeSeriesRecord: ", retMe )
    return retMe 

#
# @return the first record in accountsTimeSeries for the given account 
#
def getFirstAccountTimeSeriesRecord( account, db ):
    retMe = db.accountsTimeSeries.find_one( { "accountId": account["accountId"] },
                                            sort=[ ("timestamp", 1) ] )
    print("getFirstAccountTimeSeriesRecord: ", retMe )
    return retMe 


#
# Set account performance field ("last7days", "last30days", "last90days", "last365days")
#
def updateAccountPerformance( account, fieldName, begindate, db ):
    # the begindate is either 7daysago, 30daysago, etc.
    # search for the first record *before* that date to use for comparison to today.
    # if there's no record that early, just get the first one
    # to see why, let's imagine there's the following time-series records: 
    # from 35 days ago, balance=20
    # from 15 days ago, balance=10,
    # from 10 days ago,  balance=5
    # current balance is 5.  
    # what will the performances be?
    # last7days: 10 days ago: 5 - 5 = 0
    # last30days: 35 days ago: 5 - 20 = -15
    # last90days: 35 days ago: 5 - 20 = -15
    record = getPreviousAccountTimeSeriesRecordBeforeDate( account, begindate, db)
    if (record):
        account[fieldName] = account["value"] - record["value"]
        print("updateAccountPerformance: begindate=" + begindate.strftime("%m/%d/%y"), "PREVIOUS accountTimeSeries record=", record)
    else:
        record = getFirstAccountTimeSeriesRecord( account, db)
        if (record):
            account[fieldName] = account["value"] - record["value"]
            print("updateAccountPerformance: begindate=" + begindate.strftime("%m/%d/%y"), "FIRST accountTimeSeries record=", record)
        else:
            account[fieldName] = 0

    print("updateAccountPerformance: account=", pruneAccount(account))
    upsertAccount(db, account)  


#
# Set account performance fields ("last7days", "last30days", "last90days", "last365days")
# for all accounts.
# 
def setAccountPerformance( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    accounts = getActiveAccounts(db)

    # delta = timedelta( days=int(args["--daysago"]) )

    for daysago in [7, 30, 90, 365]:
        delta = timedelta( days=daysago )
        begindate = datetime.today() - delta
        fieldName = "last" + str(daysago) + "days"
        print("setAccountPerformance: begindate=" + begindate.strftime("%m/%d/%y"), "fieldName=" + fieldName)

        accounts.rewind()
        for account in accounts:
            updateAccountPerformance( account, fieldName, begindate, db )


#
# @return db cursor to all trans for the given account
#
def getAccountTransactions( account, db ):
    retMe = db.transactions.find( { "isPending": False,
                                    "mintMarker": 1,
                                    "fi": account["fiName"],
                                    "account": account["accountName"] } )
    print("getAccountTransactions: account=" + account["accountName"], "trans.count:", retMe.count())
    return retMe
    

#
# @return list of trans within the given timestamps
#
def filterTransInRange( trans, startTimestamp, endTimestamp ):
    retMe = list(filter(lambda tran: tran["timestamp"] >= startTimestamp and tran["timestamp"] <= endTimestamp, 
                        trans))
    print("filterTransInRange: startTimestamp:", startTimestamp, "endTimestamp:", endTimestamp )
    for tran in retMe:
        print("filterTransInRange: tran:", pruneTran(tran) )

    return retMe


#
# @return signed tran amount (negative if "isDebit")
#
def getSignedTranAmount(tran):
    sign = -1 if tran["isDebit"] else 1
    return sign * currencyToFloat( tran["amount"] )


#
# @return the sum of amounts for all given trans
#
def sumTranAmounts( trans ):
    retMe = functools.reduce( lambda memo, tran: memo + getSignedTranAmount(tran),
                              trans,
                              0 )
    print("sumTranAmounts: ", retMe )
    return retMe


#
# @return true if there exists a tran that is earlier than the given timestamp
#
def doesEarlierTranExist( trans, timestamp ):
    retMe = list(filter(lambda tran: tran["timestamp"] < timestamp, 
                        trans))
    print( "doesEarlierTranExist: timestamp:", timestamp, "len:", len(retMe) )
    return len(retMe) > 0


#
# Note: this won't be exact.  can't tell which trans were included in the earliest-time-series record
#       and which weren't.  oh well. include same-day trans or no?  YES.  Assume same-day trans are
#       already included in the earliest-time-series record. We need to include them in order to compute
#       the time-series record for 7 days ago (it's like we're rolling back the trans between now and then).
#
def backfillTimeSeries( account, db ):

    trans = list( getAccountTransactions( account, db ) )

    # Note: this algorithm goes *backward* in time... 
    #       For each account, it starts at the date of the earliest "non-backfill" record,
    #       then goes back 1-week at a time and computes "backfill" balances (by summing the
    #       trans for that week) and creates a new "backfilled" record.
    #
    #       Note: It's ok to run this multiple times against the same account, since each run
    #       will just overwrite the backfill records from previous runs.
    currRecord = getFirstNonBackfillAccountsTimeSeriesRecord( account, db )
    currTimestamp = currRecord["timestamp"] 
    
    while doesEarlierTranExist( trans, currTimestamp + 1):     # include trans == currTimestamp
        weekAgoTimestamp = currTimestamp - (7 * 86400)
        amount = sumTranAmounts( filterTransInRange( trans, weekAgoTimestamp+1, currTimestamp ) )

        weekAgoRecord = createBackfillAccountTimeSeriesRecord( account, weekAgoTimestamp, currRecord, amount )

        upsertAccountsTimeSeriesRecord( db, weekAgoRecord )  
        currTimestamp = weekAgoTimestamp
        currRecord = weekAgoRecord


#
# @return an account object wrapped around the given accountId
#
def wrapAccountId(accountId):
    retMe = {}
    retMe["accountId"] = accountId
    return retMe


#
# @return the sum value of the previous accountTimeSeries record prior to the given date
#         for all the given accounts.
#
def getPrevTimeSeriesRecordsSumValue( accounts, beforeDate, db ):
    prevTimeSeriesRecords = map( lambda account: getPreviousAccountTimeSeriesRecordBeforeDate( account, beforeDate, db), accounts )
    return sumAccountValues( prevTimeSeriesRecords )


#
# backfill summary timeseries (net worth, bank and credit, investment and other)
#
# Start at currDate = today
#   for each summary account, find prev accountTimeSeries entry prior to currDate (it's the same for all summary accounts)
#   create timeseries recourd by summing (non-summary-)account balances
#   currDate = currDate - 7 days.
#
# Loop until no more accountTimeSeries records exist earlier than the given date
# 
# 
def backfillSummaryTimeSeries( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    # only worry about active accounts for the backfill.
    accounts = list(getActiveAccounts(db))

    # Get earliest timeseries timestamp
    firstTimeSeriesRecord = db.accountsTimeSeries.find_one({},
                                                           sort=[ ("timestamp", 1) ] )
    firstTimestamp_s = firstTimeSeriesRecord["timestamp"]
    print("backfillSummaryTimeSeries: firstTimestamp_s=", firstTimestamp_s)

    # start from a week before the earliest non-backfill record, and work our way backwards.
    currRecord = getFirstNonBackfillAccountsTimeSeriesRecord( wrapAccountId(-1), db )
    currTimestamp_s = currRecord["timestamp"] - (7 * 86400)
    
    while (currTimestamp_s >= firstTimestamp_s ):     
        beforeDate = datetime.fromtimestamp(currTimestamp_s)

        sumValue = getPrevTimeSeriesRecordsSumValue( accounts, beforeDate, db )
        record = createBackfillSummaryTimeSeriesRecord( -1, "Net Worth", beforeDate, sumValue)
        upsertAccountsTimeSeriesRecord( db, record )  

        sumValue = getPrevTimeSeriesRecordsSumValue( filterBankAndCreditAccounts(accounts), beforeDate, db )
        record = createBackfillSummaryTimeSeriesRecord( -2, "Bank & Credit Accounts", beforeDate, sumValue)
        upsertAccountsTimeSeriesRecord( db, record )  

        sumValue = getPrevTimeSeriesRecordsSumValue( filterNonBankAndCreditAccounts(accounts), beforeDate, db )
        record = createBackfillSummaryTimeSeriesRecord( -3, "Investment & Other Accounts", beforeDate, sumValue)
        upsertAccountsTimeSeriesRecord( db, record )  

        # move back 7 days
        currTimestamp_s = currTimestamp_s - (7 * 86400)


#
# Backfill account timeseries records.
#
# For each account...
#   Find earliest accountsTimeSeries entry
#     Check if there are any earlier transactions
#     If yes, get trans between 'earliest accountsTimeSeries date' and 7 days prior
#     Sum total value of trans
#     Create new accountsTimeSeries entry
#   Loop
# Loop
# 
def backfillAccountsTimeSeries( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )
    accounts = getActiveBankAndCreditAccounts(db)

    for account in accounts:
        print( "backfillAccountsTimeSeries: accountName=" + account["accountName"], "fiLastUpdated=", account["fiLastUpdated"], "datestr=" + formatDateString_ms( account["fiLastUpdated"] ) )
        backfillTimeSeries( account, db )


#
# Remove unused tags.
# 
def refreshTags( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )
    trans = db.transactions.find({ "tags": { "$exists": True, "$ne": [] } }, projection= { "tags": True } );

    print("refreshTags: trans.count=", trans.count())

    tranSet = set()

    for tran in trans:
        for tag in tran["tags"]:
            tranSet.add( tag ) 

    print( "refreshTags: transaction tags=", tranSet );
    db.tags.update_one( { "_id": 1 }, 
                        { "$set": { "tags": list(tranSet) } } )

    dbtags = db.tags.find_one()["tags"];
    print("refreshTags: after update: dbtags=", dbtags)
    print("refreshTags: after update: dbtags - tranSet =", (set(dbtags) - tranSet))


#
# Copy the tags from fromTran to toTran.
#
def applyTags( fromTran, toTran ):
    tags = toTran.setdefault("tags",[])
    for tag in fromTran.setdefault("tags",[]):
        if (tag not in tags):
            tags.append(tag)


#
# Apply automatic tags based on tran data
#
def applyAutoTags(tran):
    if (tran["txnType"] == 1):
        tags = tran.setdefault("tags",[])
        if ("investment" not in tags):
            tags.append("investment")

#
# Auto-tag the given tran with the tags from the previous tran
# from the same merchant.
#
def applyPrevTranTags(tran, db):
    print("applyPrevTranTags: searching for prev tran for tran:", pruneTran(tran))
    prevTran = db.transactions.find_one( {  "merchant": tran["merchant"],
                                            "tags": { "$exists": True, "$ne": [] },
                                            "hasBeenAcked": True,  # TODO: do i care if it's been acked already? 
                                            "timestamp": { "$lte": tran["timestamp"] }
                                         },
                                         sort=[ ("timestamp", -1) ] );
    if prevTran is not None:
        print("applyPrevTranTags: prev tran:", pruneTran(prevTran)) 
        applyTags(prevTran, tran);

    
#
# Auto-tag non-ack'ed trans.
#
def autoTagTrans( args ):
    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )
    trans = getNonAckedTransactions( db, sort=[ ("timestamp", 1) ] )

    for tran in trans:
        alreadyTagged = (len( tran.setdefault("tags",[]) ) > 0)
        applyAutoTags(tran)

        # don't copy prev tran tags if this tran has already been tagged by the user
        if (alreadyTagged == False):
            applyPrevTranTags(tran, db)

        updateTran(tran, db)   

#
# backfill auto tags
#
def backfillAutoTags( args ):
    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )
    trans = db.transactions.find({}, projection={ "_id": True, "txnType": True, "tags": True } );
    for tran in trans:
        applyAutoTags(tran)
        updateTran(tran, db)  


#
# @return the new copy of the given "marooned" tran
#
def findNewTranCopy(tran, db):

    # TODO: what if two trans are identical in every way except tags?
    #       that means there will be two dups.
    #       so all we need to do is make sure 1 dup goes to 1 tran and the other goes the other.
    #       that means we need to mark the "dup'ed" tran as "resolved".  or just delete it altogether.
    # TODO: not matching on merchant?  sometimes merchant changes?
    print("findNewTranCopy: tran=", pruneTran(tran))
    retMe = db.transactions.find_one(  {     "mintMarker" : 1,
                                             "hasBeenAcked": False,
                                             "isPending": tran["isPending"],
                                             "account": tran["account"],
                                             "fi": tran["fi"],
                                             "amount": tran["amount"],
                                             "date": tran["date"]
                                       } )
    print("findNewTranCopy: retMe=", pruneTran(retMe))
    return retMe


#
# Transfer tags and other data from fromTran to toTran
#
def transferTranData(fromTran, toTran):
    applyTags(fromTran, toTran)
    toTran["hasBeenAcked"] = fromTran["hasBeenAcked"]
    print("transferTranData: toTran:", pruneTran(toTran))


#
# Sometimes mint re-creates an entire account record, and in doing
# so makes a copy of a bunch of transactions associated with that account,
# then deletes the old copies of both the account and transactions.
# 
# These copied transactions show up as new, non-acked trans in thinmint.
# The old copies of the trans (the "marooned" trans) still exist in thinmint, 
# with all their tags still applied.
#
# The purpose of this method is to find all those marooned trans, find their
# corresponding new copy, and transfer the tag data from the marooned tran
# to the new tran.
# 
def syncMaroonedTrans( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    trans = db.transactions.find(  { "mintMarker" : 0,
                                     "hasBeenAcked": True,
                                     "isPending": False } )

    print("syncMaroonedTrans: marooned tran count:", trans.count())

    syncCount = 0

    for tran in trans:
        newTran = findNewTranCopy( tran, db )
        if newTran is not None:
            transferTranData( tran, newTran )
            updateTran( newTran, db )
            syncCount = syncCount + 1

    print("syncMaroonedTrans: exit, sync count=", syncCount)


#
# @return the string s, padded on the right to the nearest multiple of bs.
#         the pad char is the ascii char for the pad length.
#
def pad(s, bs):
    retMe = s + (bs - len(s) % bs) * chr(bs - len(s) % bs)
    # print("pad: retMe: #" + retMe + "#")
    return retMe


#
# @param s a string or byte[] previously returned by pad. 
#          assumes the pad char is equal to the length of the pad
#
# @return s with the pad on the right removed.
#
def unpad(s):
    retMe = s[:-ord(s[len(s)-1:])]
    # print("unpad: retMe:", retMe)
    return retMe


#
# @param key - key size can be 16, 24, or 32 bytes (128, 192, 256 bits)
#              You must use the same key when encrypting and decrypting.
# @param msg - the msg to encrypt
#
# @return base64-encoded ciphertext
#
def encrypt(key, msg):
    msg = pad(msg, AES.block_size)

    #
    # iv is like a salt.  it's used for randomizing the encryption
    # such that the same input msg isn't encoded to the same cipher text
    # (so long as you use a different iv).  The iv is then prepended to
    # the ciphertext.  Before decrypting, you must remove the iv and only
    # decrypt the ciphertext.
    #
    # Note: AES.block_size is always 16 bytes (128 bits)
    #
    iv = Random.new().read(AES.block_size)

    cipher = AES.new(key, AES.MODE_CBC, iv)

    #
    # Note: the iv is prepended to the encrypted message
    # encryptedMsg is a base64-encoded byte[] 
    # 
    return base64.b64encode(iv + cipher.encrypt(msg))


#
# @param key - key size can be 16, 24, or 32 bytes (128, 192, 256 bits)
#              You must use the same key when encrypting and decrypting.
# @param encryptedMsg - the msg to decrypt (base64-encoded), previously returned 
#                       by encrypt.  First 16 bytes is the iv (salt)
#
def decrypt(key, encryptedMsg):
    enc = base64.b64decode(encryptedMsg)
    iv = enc[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')


#
# TODO: not strong enough.  need salt and multiple hashes
# @return a hexdigest representing the hashed password
#
def hashPassword( password ):
    m = hashlib.md5()
    m.update(password.encode("utf-8"))
    return m.hexdigest()


#
# @return AES-encrypted mintCred
#
def encryptCreds( mintCred ):
    key = os.environ['TM_AES_KEY'].encode('utf-8')
    return encrypt(key, mintCred).decode('utf-8')


#
# @return AES-derypted mintCred
#
def decryptCreds( encMintCred ):
    key = os.environ['TM_AES_KEY'].encode('utf-8')
    return decrypt(key, encMintCred)


#
# Add a user to the DB
# 
def addUser( args ):

    db = getMongoDb( args["--mongouri"] )

    user = args["--user"]
    password = args["--pass"]
    hashedPassword = hashPassword( password )

    mintCred = args['--mintuser'] + ":" + args['--mintpass']
    encMintCred = encryptCreds(mintCred)

    print("addUser: user=" + user + ", hashedPassword=" + hashedPassword + ", encMintCred=" + encMintCred);

    db["/tm/users"].update_one( { "_id": user }, 
                                { "$set": { "password": hashedPassword,
                                            "mintCred": encMintCred } 
                                }, 
                                upsert=True )

#
# Set fields --mintuser and --mintpass into the given args object.
# The mint creds are pulled from the DB
#
# @return args
#
def addMintCreds( user, args ):

    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    db = getMongoDb( args["--mongouri"] )
    userRecord = db["/tm/users"].find_one( { "_id": user } )

    if (userRecord is not None):
        print("addMintCreds: userRecord:", userRecord)

        mintCred = decryptCreds( userRecord["mintCred"] )
        args["--mintuser"] = mintCred.split(":",1)[0]
        args["--mintpass"] = mintCred.split(":",1)[1]
        args["--user"] = user;
        
        # print("addMintCreds: mintCred:", mintCred, "args:", args)

    return args



#
# @param trans cursor
#
# @return map of maps: byTagByMonth[tag][yearMonth] = sum(amountValue)
#
def groupByTagByMonth( trans ):
    retMe = {}

    for tran in trans:
        yearMonth = datetime.fromtimestamp( tran["timestamp"] ).strftime( "%Y.%m" )

        for tag in ( tran["tags"] if "tags" in tran and tran["tags"] else ["(untagged)"] ):
            byTag = retMe.setdefault(tag, {})
            byMonth = byTag.setdefault(yearMonth,{})
            byMonth["sumAmountValue"] = byMonth.setdefault("sumAmountValue",0) + tran["amountValue"]
            byMonth["countTrans"] = byMonth.setdefault("countTrans",0) + 1

    return retMe


#
# @return a record for transByTagByMonth collection
#
def createTagsByMonthRecord( tag, yearMonth, byMonthData ):
    return { "_id": tag + "-" + yearMonth, 
             "tag": tag,
             "yearMonth": yearMonth,
             "sumAmountValue": byMonthData["sumAmountValue"],
             "countTrans": byMonthData["countTrans"] }


#
# Upsert record.
#
def upsertTagsByMonth( db, record ):
    print("upsertTagsByMonth: record:", record )
    db.tagsByMonth.update_one( { "_id": record["_id"] }, 
                               { "$set": record }, 
                               upsert=True )

#
# Group all tran amounts by tag and by month, to get a breakdown of 
# spending per tag per month.
#
def groupTransByTagByMonth( args ):

    db = getUserDb( getMongoDb( args["--mongouri"] ), args["--user"] )

    # Clear the entire tagsByMonth table.
    # By clearing the table we get rid of stale defunct tags 
    # The code below rebuilds the full table.
    db.tagsByMonth.remove({})

    # Get all trans. Untagged trans will be grouped under tag="(untagged)"  
    trans = db.transactions.find({ 
                                   "mintMarker": 1 
                                 }, 
                                 projection= { "tags": True, 
                                               "amountValue": True,
                                               "timestamp": True 
                                             })
    #
    # byTagByMonth[tag][yearMonth] = sum(amountValue)
    #
    byTagByMonth = groupByTagByMonth(trans)
    print("groupByTagByMonth: byTagByMonth:", json.dumps( byTagByMonth, indent=2, sort_keys=True)  )

    for tag in byTagByMonth:
        for yearMonth in byTagByMonth[tag]:
            upsertTagsByMonth( db, createTagsByMonthRecord(tag, yearMonth, byTagByMonth[tag][yearMonth] ) )



#
# main entry point ---------------------------------------------------------------------------
# 
args = verifyArgs( parseArgs() , required_args = [ '--action' ] )
# -rx- print("main: verified args=", args)

args = setMongoUri(args)

# TODO: read user list from DB.
user="ilana.bram@gmail.com"
args = setUser(args, user)


if args["--action"] == "getMintAccounts":
    args = verifyArgs( args , required_args = [ '--mintuser', '--mintpass', '--outputfile' ] )
    doGetMintAccounts( args )

elif args["--action"] == "refreshMintAccounts":
    args = addMintCreds( user, args)
    args = verifyArgs( args , required_args = [ '--mintuser', '--mintpass' ] )
    refreshMintAccounts( args )

elif args["--action"] == "getMintTransactions":
    args = verifyArgs( args , required_args = [ '--mintuser', '--mintpass', '--outputfile' ] )
    doGetMintTransactions( args )

elif args["--action"] == "readTransactions":
    args = verifyArgs( args , required_args = [ '--inputfile' ] )
    doReadTransactions( args )

elif args["--action"] == "readAccounts":
    args = verifyArgs( args , required_args = [ '--inputfile' ] )
    doReadAccounts( args )

elif args["--action"] == "convertTransactionsToMap":
    args = verifyArgs( args , required_args = [ '--inputfile', '--outputfile' ] )
    doConvertTransactionsToMap( args )

elif args["--action"] == "mergeTransactions":
    args = verifyArgs( args , required_args = [ '--mintfile', '--inputfile', '--outputfile' ] )
    doMergeTransactions( args )

elif args["--action"] == "setHasBeenAcked":
    args = verifyArgs( args , required_args = [ '--inputfile', '--outputfile' ] )
    writeJson( forEachTransactionMap( readJson( args[ '--inputfile' ] ), setHasBeenAcked), args[ '--outputfile' ] )

elif args["--action"] == "setHasBeenAckedMint":
    args = verifyArgs( args , required_args = [ '--inputfile', '--outputfile' ] )
    writeJson( list( map( setHasBeenAcked, readJson( args[ '--inputfile' ] ) ) ), args[ '--outputfile' ] )

elif args["--action"] == "composeEmailSummary_OLD":
    args = verifyArgs( args , required_args = [ '--transfile', '--accountsfile', '--outputfile' ] )
    doComposeEmailSummary_OLD( args )

elif args["--action"] == "composeEmailSummary":
    args = verifyArgs( args , required_args = [ '--user', '--outputfile' ] )
    doComposeEmailSummary( args )

elif args["--action"] == "sendEmailSummary":
    args = verifyArgs( args , required_args = [ '--inputfile', '--gmailuser', '--gmailpass', '--to' ] )
    sendEmailSummary( args )

elif args["--action"] == "composeAndSendEmailSummary":
    args = verifyArgs( args , required_args = [ '--mongouri', '--user' ] )
    composeAndSendEmailSummary( args )

elif args["--action"] == "importMintDataToMongo":
    args = addMintCreds( user, args)
    args = verifyArgs( args , required_args = [ '--mongouri', '--user', '--mintuser', '--mintpass' ] )
    importMintDataToMongo( args )

elif args["--action"] == "resolvePendingTransactions":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    resolvePendingTransactions( args )

elif args["--action"] == "setTransactionTimestamps":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    setTransactionTimestamps( args )

elif args["--action"] == "setTransactionAmountValues":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    setTransactionAmountValues( args )

elif args["--action"] == "setAccountsTimeSeriesTimestamps":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    setAccountsTimeSeriesTimestamps( args )

elif args["--action"] == "setAccountPerformance":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    setAccountPerformance( args );

elif args["--action"] == "backfillAccountsTimeSeries":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    backfillAccountsTimeSeries( args );

elif args["--action"] == "refreshTags":
    args = verifyArgs( args , required_args = ['--user',  '--mongouri' ] )
    refreshTags( args );

elif args["--action"] == "autoTagTrans":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    autoTagTrans( args );

elif args["--action"] == "addUser":
    args = verifyArgs( args , required_args = [ '--mongouri', '--user', '--pass', '--mintuser', '--mintpass' ] )
    addUser( args );

elif args["--action"] == "addMintCreds":
    args = verifyArgs( args , required_args = [ '--mongouri', '--user' ] )
    addMintCreds( args['--user'], args );

elif args["--action"] == "backfillAutoTags":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    backfillAutoTags( args );

elif args["--action"] == "syncRemovedPendingTrans":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    syncRemovedPendingTrans( args );

elif args["--action"] == "syncMaroonedTrans":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    syncMaroonedTrans( args );

elif args["--action"] == "checkUserDb":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    checkUserDb( args );

elif args["--action"] == "backfillSummaryTimeSeries":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    backfillSummaryTimeSeries( args );

elif args["--action"] == "doUpsertSummaryTimeSeries":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    doUpsertSummaryTimeSeries( args );

elif args["--action"] == "groupTransByTagByMonth":
    args = verifyArgs( args , required_args = [ '--user', '--mongouri' ] )
    groupTransByTagByMonth( args );




else:
    print ( "main: Unrecognized action: " + args["--action" ] )


