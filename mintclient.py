#! /usr/bin/python3
# @rob4lderman
# ThinMint utils. 
#
# Downloads trans from mint, merges with thinmint db, sends summary email.
#
#
# Usage:
#
# Download mint trans and accounts:
#   $ python3 mintclient.py --action getMintTransactions --mintuser . --mintpass . --outputfile=trans.mint.json
#   $ python3 mintclient.py --action getMintAccounts --mintuser . --mintpass . --outputfile=accounts.mint.json
#
# Convert initial mint trans download to thinmint trans (only do this once. subsequently use mergeTransactions)
#   $ python3 mintclient.py --action convertTransactionsToMap --inputfile=trans.mint.json --outputfile=trans.thinmint.json
#
# Set hasBeenAcked=true for all initial trans (only do this once).
#   $ python3 mintclient.py --action setHasBeenAcked --inputfile=trans.thinmint.json --outputfile=trans.thinmint.json
#
# Download new mint trans, merge with existing thinmint trans
#   $ python3 mintclient.py --action getMintTransactions --mintuser . --mintpass . --outputfile=data/trans.mint.json
#   $ python3 mintclient.py --action mergeTransactions --mintfile=data/trans.mint.json --inputfile=data/trans.thinmint.json --outputfile=data/trans.thinmint.json
#
# Send email with status update, new trans in need of ACK'ing
#   $ python3 mintclient.py --action composeEmailSummary --transfile=trans.thinmint.json --accountsfile=accounts.mint.json --outputfile=email.txt
#   $ python3 mintclient.py --action sendEmailSummary ---inputfile=email.txt --to . --gmailuser . --gmailpass .
# 
# 
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
import mintapi
import getopt
import sys
import re
import functools

from datetime import date, datetime, timedelta

from mailer import Mailer
from mailer import Message

from pymongo import MongoClient

import locale
locale.setlocale( locale.LC_ALL, '' )


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
                     "daysago="]
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


#
# @return accounts data in thinmint format
#
def convertAccounts( accounts ):
    accounts = removeDatetime(accounts)
    for account in accounts:
        convertAccount( account )
    return accounts


#
# Retrieve account data from mint
# @return accounts object
#
def getMintAccounts( args ):
    print( "getMintAccounts: Logging into mint..." )
    mint = mintapi.Mint( args["--mintuser"], args["--mintpass"] )

    print( "getMintAccounts: Refreshing accounts..." )
    mint.initiate_account_refresh()

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
    mint = mintapi.Mint( args["--mintuser"], args["--mintpass"] )

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
# @return timestamp
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
    return {k: tran.get(k) for k in ('id', 'account', 'amount', 'fi', 'date', 'timestamp', 'isPending', 'hasBeenAcked', 'isDebit', 'merchant', 'linkedTranIds', 'isResolved', 'tags')}

#
# @return a subset of fields, typically for printing.
#
def pruneAccount( account ):
    return {k: account.get(k) for k in ('accountId', 'accountName', 'fiName', 'accountType', 'currentBalance', 'value', 'isActive', 'lastUpdated', 'lastUpdatedInString')}


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
    retMe.append( "Summary of new transactions:")
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
    retMe.append("<b>Summary of new transactions</b><br/>")
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
def getNonAckedTransactions( db ):
    trans = db.transactions.find( { "$or": [ { "hasBeenAcked": { "$exists": False } }, { "hasBeenAcked" : False } ] } )
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
    accounts = db.accounts.find( { "isActive" : True } )
    print("getActiveAccounts: Found {} active bank/credit accounts out of {}".format( accounts.count(), db.accounts.count() ) )
    return accounts


#
# Compose an email summary and write it to the --outputfile
#
def doComposeEmailSummary( args ) : 

    db = getMongoDb( args["--mongouri"]  )

    # read trans that have yet to be acked.
    trans = getNonAckedTransactions( db )

    # read active bank and credit card accounts
    accounts = getActiveBankAndCreditAccounts( db )
    
    writeLines( composeHtmlEmail( accounts, trans ), args["--outputfile"] + ".html" )

    # need to rewind the cursors
    trans.rewind()
    accounts.rewind()
    writeLines( composeTextEmail( accounts, trans ), args["--outputfile"] )



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
# @return a ref to the mongo db by the given name.
#
def getMongoDb( mongoUri ):
    dbname = mongoUri.split("/")[-1]
    mongoClient = MongoClient( mongoUri )
    print("getMongoDb: connected to {}, database {}".format( mongoUri, dbname ) )
    return mongoClient[dbname]


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
    return str(account["_id"]) + "." + datestr


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
# Insert or update account time-series records into mongo.
# Time-series records keep track of account balance from day-to-day.
#
def upsertAccountsTimeSeries( db, accounts ):
    for account in accounts:
        record = createAccountTimeSeriesRecord( account )
        upsertAccountsTimeSeriesRecord( db, record )

    print("upsertAccountsTimeSeries: db.accountsTimeSeries.count()=", db.accountsTimeSeries.count())


#
# Insert or update mint transaction records into mongo.
#
def upsertTransactions( db, trans ):
    for tran in trans:
        db.transactions.update_one( { "_id": tran["_id"] }, 
                                    { "$set": tran }, 
                                    upsert=True )
    print("upsertTransactions: db.transactions.count()=", db.transactions.count())


#
# @return subset of accounts with isActive=True
#
def filterActiveAccounts( accounts ):
    return list( filter(lambda act: act["isActive"], accounts) ) 


#
# Download all accounts and trans from mint.
# Push mint accounts into db.accounts
#   Note: existing account data will be overwritten.  This is OK since I don't intend to modify ANY mint data.
#
def importMintDataToMongo( args ):
    # make sure we can reach mongo first
    db = getMongoDb( args["--mongouri"] )

    mintAccounts = convertAccounts( getMintAccounts( args ) )
    mintTrans = convertTransactions( getMintTransactions( args ) )

    upsertAccounts( db, mintAccounts )
    upsertAccountsTimeSeries( db, filterActiveAccounts( mintAccounts ) )

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
        return True
    elif (float1 > float2):
        return float1 < (float2 + tolerance)
    elif (float1 < float2):
        return float1 > (float2 - tolerance)


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
def isPendingTranLikelyMatch( pendingTran, tran ):
    return isMerchantMatch( pendingTran["merchant"], tran["merchant"] ) and isFloatWithin( currencyToFloat( tran["amount"] ),
                                                                                           currencyToFloat( pendingTran["amount"] ),
                                                                                           currencyToFloat( pendingTran["amount"] ) * 0.30 )
#
# @return true if the merchant and amount matches.
#
def isPendingTranMatch( pendingTran, tran ):
    return isMerchantMatch( pendingTran["merchant"], tran["merchant"] ) and (tran["amount"] == pendingTran["amount"])


#
# Add tran1 to tran2['linkedTranIds']
#
# @sideeffect: tran2 is modified.
#
def linkTranTo( tran1, tran2):
    linkedTranIds = tran2.setdefault('linkedTranIds',[])
    if (tran1["_id"] not in linkedTranIds):
        linkedTranIds.append(tran1["_id"])


#
# Link the two trans in their linkedTranIds field.
# Copy the tags from the pendingTran to the clearedTran.
# Set pendingTran.isMerged = True.
#
# @sideeffect: pendingTran and clearedTran are modified
#
def linkPendingTran( pendingTran, clearedTran ):
    
    linkTranTo(pendingTran, clearedTran)
    linkTranTo(clearedTran, pendingTran)

    applyTags( pendingTran, clearedTran )
    # tags = clearedTran.setdefault('tags',[])
    # for tag in pendingTran.setdefault('tags',[]):
    #     if (tag not in tags):
    #         tags.append(tag)

    pendingTran['isResolved'] = True
    pendingTran['hasBeenAcked'] = True

    print("linkPendingTran: pendingTran: ", pruneTran(pendingTran) )
    print("linkPendingTran: clearedTran: " , pruneTran(clearedTran) )


#
# Do a full update of the given tran in the db.
#
def updateTran( tran, db ):
    db.transactions.update_one( { "_id": tran["_id"] }, 
                                { "$set": tran } )


#
# @return a list of clearedTrans that are likely matches for the given pendingTran
#
def resolvePendingTran( pendingTran, db ):

    trans = db.transactions.find( { "isPending" : False, 
                                    "fi": pendingTran["fi"], 
                                    "account": pendingTran["account"],
                                    "timestamp": { "$lte": (pendingTran["timestamp"] + (86400 * 7) ),   # cleared within a week
                                                   "$gte": pendingTran["timestamp"] }                # TODO: use pendingTran["timestamp"] eventually
                                  } ) 

    print("resolvePendingTran: Searching for potential matches for ", formatNewTranText(pendingTran) , ". Potential match count: ", trans.count());

    # first look for exact matches
    matches = list( filter( lambda tran: isPendingTranMatch(pendingTran, tran), trans ) )

    if ( len(matches) == 0 ): 
        # no exact matches... look for 'likely' matches..
        trans.rewind()
        matches = list( filter( lambda tran: isPendingTranLikelyMatch(pendingTran, tran), trans ) )

    if ( len(matches) > 0 ): 
        print("resolvePendingTran: ====> match:", formatNewTranText( matches[0] ) )

    return matches


#
# For all pending trans not yet resolved...
# Look for matches
# Link matches to pendingTran
# Push pendingTran tags to matches
# Mark pendingTran resolved.
#
def resolvePendingTransactions( args ):
    db = getMongoDb( args["--mongouri"] )

    # pendingTrans = db.transactions.find( { "isPending" : True } ) 
    pendingTrans = db.transactions.find( { "isPending" : True, 
                                           "isResolved": { "$exists": False } } )

    for pendingTran in pendingTrans:
        clearedTrans = resolvePendingTran( pendingTran, db )
        for clearedTran in clearedTrans:
            linkPendingTran( pendingTran, clearedTran )
            updateTran(clearedTran, db)
        updateTran(pendingTran,db)


#
# Set timestamp field in all trans that don't have one
#
def setTransactionTimestamps( args ):
    db = getMongoDb( args["--mongouri"] )

    trans = db.transactions.find( { "timestamp": { "$exists": False } } )

    print("setTransactionTimestamps: tran count:", trans.count())

    for tran in trans:
        tran["timestamp"] = getTimestamp( tran["date"] )
        updateTran(tran, db)

#
# Set transaction amountValue
#
def setTransactionAmountValues( args ):
    db = getMongoDb( args["--mongouri"] )

    trans = db.transactions.find( { "amountValue": { "$exists": False } } )

    print("setTransactionAmountValues: tran count:", trans.count())

    for tran in trans:
        tran["amountValue"] = getSignedTranAmount( tran )
        updateTran(tran, db)


#
# Set timestamp field in all accountsTimeSeries that don't have one.
#
def setAccountsTimeSeriesTimestamps( args ):

    db = getMongoDb( args["--mongouri"] )

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
# @return the first record in accountsTimeSeries for the given account after the given date
#
def findEarliestAccountsTimeSeriesRecord( account, db ):
    retMe = db.accountsTimeSeries.find_one( { "accountId": account["accountId"],
                                              "isBackfill": { "$exists": False } },
                                             sort=[ ("timestamp", 1) ] )
    print("findEarliestAccountsTimeSeriesRecord: ", retMe )
    return retMe 


#
# Set account performance field ("7daysago", "30daysago", "90daysago", "365daysago")
#
def updateAccountPerformance( account, fieldName, begindate, db ):
    record = getFirstAccountTimeSeriesRecordAfterDate( account, begindate, db)
    if (record):
        account[fieldName] = account["value"] - record["value"]
        print("updateAccountPerformance: accountTimeSeries record=", record)
        print("updateAccountPerformance: account=", account)
        upsertAccount(db, account)


#
# Set account performance fields ("7daysago", "30daysago", "90daysago", "365daysago")
# for all accounts.
# 
def setAccountPerformance( args ):

    db = getMongoDb( args["--mongouri"] )
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
# TODO: this won't be exact.  can't tell which trans were included in the earliest-time-series record
#       and which weren't.  oh well. include same-day trans or no?  YES.  Assume same-day trans are
#       already included in the earliest-time-series record. We need to include them in order to compute
#       the time-series record for 7 days ago (it's like we're rolling back the trans between now and then).
#
def backfillTimeSeries( account, db ):

    trans = list( getAccountTransactions( account, db ) )

    # Note: this algorithm goes *backward* in time... 
    currRecord = findEarliestAccountsTimeSeriesRecord( account, db )
    currTimestamp = currRecord["timestamp"] 
    
    while doesEarlierTranExist( trans, currTimestamp + 1):     # include trans == currTimestamp
        weekAgoTimestamp = currTimestamp - (7 * 86400)
        amount = sumTranAmounts( filterTransInRange( trans, weekAgoTimestamp+1, currTimestamp ) )

        weekAgoRecord = createBackfillAccountTimeSeriesRecord( account, weekAgoTimestamp, currRecord, amount )

        upsertAccountsTimeSeriesRecord( db, weekAgoRecord )
        currTimestamp = weekAgoTimestamp
        currRecord = weekAgoRecord


#
# Backfill account performance fields ("7daysago", "30daysago", "90daysago", "365daysago").
#
# For each account...
# Find earliest accountsTimeSeries entry
# Check if there are any earlier transactions
# If yes, get trans between 'earliest accountsTimeSeries date' and 7 days prior
# Sum total value of trans
# Create new accountsTimeSeries entry
# Loop
# 
def backfillAccountsTimeSeries( args ):

    db = getMongoDb( args["--mongouri"] )
    accounts = getActiveBankAndCreditAccounts(db)

    for account in accounts:
        print( "backfillAccountsTimeSeries: accountName=" + account["accountName"], "fiLastUpdated=", account["fiLastUpdated"], "datestr=" + formatDateString_ms( account["fiLastUpdated"] ) )
        backfillTimeSeries( account, db )


#
# Remove unused tags.
# 
def removeUnusedTags( args ):

    db = getMongoDb( args["--mongouri"] )
    trans = db.transactions.find({ "tags": { "$exists": True, "$ne": [] } }, projection= { "tags": True } );

    print("removeUnusedTags: trans.count=", trans.count())

    tranSet = set()

    for tran in trans:
        for tag in tran["tags"]:
            tranSet.add( tag ) 

    print( "removeUnusedTags: transaction tags=", tranSet );
    db.tags.update_one( { "_id": 1 }, 
                        { "$set": { "tags": list(tranSet) } } )

    dbtags = db.tags.find_one()["tags"];
    print("removeUnusedTags: after update: dbtags=", dbtags)
    print("removeUnusedTags: after update: dbtags - tranSet =", (set(dbtags) - tranSet))


#
# Copy the tags from fromTran to toTran.
#
def applyTags( fromTran, toTran ):
    tags = toTran.setdefault("tags",[])
    for tag in fromTran.setdefault("tags",[]):
        if (tag not in tags):
            tags.append(tag)


#
# Auto-tag non-ack'ed trans.
#
def autoTagTrans( args ):
    db = getMongoDb( args["--mongouri"] )
    trans = getNonAckedTransactions( db )

    for tran in trans:
        prevTran = db.transactions.find_one( {  "merchant": tran["merchant"],
                                                "tags": { "$exists": True, "$ne": [] },
                                                "hasBeenAcked": True
                                             },
                                             sort=[ ("timestamp", -1) ] );
        if prevTran is not None:
            print("autoTagTrans: tran:", pruneTran(tran))
            print("autoTagTrans: prev tran:", pruneTran(prevTran)) 
            applyTags(prevTran, tran);
            updateTran(tran, db)



#
# main entry point ---------------------------------------------------------------------------
# 
args = verifyArgs( parseArgs() , required_args = [ '--action' ] )
# -rx- print("main: verified args=", args)


if args["--action"] == "getMintAccounts":
    args = verifyArgs( args , required_args = [ '--mintuser', '--mintpass', '--outputfile' ] )
    doGetMintAccounts( args )

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
    args = verifyArgs( args , required_args = [ '--mongouri', '--outputfile' ] )
    doComposeEmailSummary( args )

elif args["--action"] == "sendEmailSummary":
    args = verifyArgs( args , required_args = [ '--inputfile', '--gmailuser', '--gmailpass', '--to' ] )
    sendEmailSummary( args )

elif args["--action"] == "importMintDataToMongo":
    args = verifyArgs( args , required_args = [ '--mongouri', '--mintuser', '--mintpass' ] )
    importMintDataToMongo( args )

elif args["--action"] == "resolvePendingTransactions":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    resolvePendingTransactions( args )

elif args["--action"] == "setTransactionTimestamps":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    setTransactionTimestamps( args )

elif args["--action"] == "setTransactionAmountValues":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    setTransactionAmountValues( args )

elif args["--action"] == "setAccountsTimeSeriesTimestamps":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    setAccountsTimeSeriesTimestamps( args )

elif args["--action"] == "setAccountPerformance":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    setAccountPerformance( args );

elif args["--action"] == "backfillAccountsTimeSeries":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    backfillAccountsTimeSeries( args );

elif args["--action"] == "removeUnusedTags":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    removeUnusedTags( args );

elif args["--action"] == "autoTagTrans":
    args = verifyArgs( args , required_args = [ '--mongouri' ] )
    autoTagTrans( args );


else:
    print ( "main: Unrecognized action: " + args["--action" ] )


