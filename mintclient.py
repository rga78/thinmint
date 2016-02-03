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
#   $ python3 mintclient.py --action getTransactions --email . --password . --outputfile=trans.mint.json
#   $ python3 mintclient.py --action getAccounts --email . --password . --outputfile=accounts.mint.json
#
# Convert initial mint trans download to thinmint trans (only do this once. subsequently use mergeTransactions)
#   $ python3 mintclient.py --action convertTransactions --inputfile=trans.mint.json --outputfile=trans.thinmint.json
#
# Set hasBeenAcked=true for all initial trans (only do this once).
#   $ python3 mintclient.py --action setHasBeenAcked --inputfile=trans.thinmint.json --outputfile=trans.thinmint.json
#
# Download new mint trans, merge with existing thinmint trans
#   $ python3 mintclient.py --action getTransactions --email . --password . --outputfile=data/trans.mint.json
#   $ python3 mintclient.py --action mergeTransactions --mintfile=data/trans.mint.json --inputfile=data/trans.thinmint.json --outputfile=data/trans.thinmint.json
#
# Send email with status update, new trans in need of ACK'ing
#   $ python3 mintclient.py --action composeEmailSummary --transfile=trans.thinmint.json --accountsfile=accounts.mint.json --outputfile=email.txt
#   $ python3 mintclient.py --action sendEmailSummary ---inputfile=email.txt --to . --gmailuser . --gmailpassword .
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

from datetime import date, datetime

from mailer import Mailer
from mailer import Message

import locale
locale.setlocale( locale.LC_ALL, '' )


#
# Parse command line args
#
# @return a dictionary of opt=val
#
def parseArgs():
    long_options = [ "email=", 
                     "password=", 
                     "action=", 
                     "startdate=", 
                     "inputfile=", 
                     "outputfile=", 
                     "mintfile=",
                     "transfile=",
                     "accountsfile=",
                     "gmailuser=",
                     "gmailpassword=",
                     "to="]
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
# Retrieve account data from mint
# @return accounts object
#
def getAccounts( args ):
    print( "getAccounts: Logging into mint..." )
    mint = mintapi.Mint( args["--email"], args["--password"] )

    print( "getAccounts: Refreshing accounts..." )
    mint.initiate_account_refresh()

    print( "getAccounts: Getting accounts..." )
    accounts = mint.get_accounts(True)  # True - get extended account detail (takes longer)
    return removeDatetime( accounts )


#
# --action getAccounts
# 
def doGetAccounts( args ):
    accounts = getAccounts(args)
    writeJson( accounts, args["--outputfile"] )


#
# Retrieve transaction data from mint
# @return transactions object
# 
def getTransactions( args ):
    print( "getTransactions: Logging into mint..." )
    mint = mintapi.Mint( args["--email"], args["--password"] )

    print( "getTransactions: Getting transactions..." )
    trxs = mint.get_transactions_json(include_investment=False, skip_duplicates=False )     # TODO: start_date
    print( "getTransactions: Transactions gotten: ", len(trxs) )
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
# --action getTransactions
# 
def doGetTransactions( args ):
    trxs = getTransactions(args)
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
# @return the trx object converted from mint style to thinmint style.
# 
def convertTransaction( trx, regex = re.compile( r'\d\d/\d\d/\d\d' )):
    trx["date"] = convertDate( trx["date"], regex )
    return trx


#
# Convert trxs objects from mint-style to thinmint-style.
# mint-style trxs are held an array.
# thinmint-style trxs are held in a dict with each trx mapped by its id.
#
# @return thinmint trxs
#
def convertTransactions( trxs ):
    regex = re.compile( r'\d\d/\d\d/\d\d' )     # for date conversions
    retMe = {}
    for trx in trxs:
        retMe[ str(trx["id"]) ] = convertTransaction( trx, regex )
    return retMe


#
# --action convertTransactions
#
def doConvertTransactions( args ):
    trxs = readJson( args["--inputfile"] )  
    print("doConvertTransactions: read transactions: ", len(trxs))
    trxs = convertTransactions( trxs )
    writeJson( trxs, args["--outputfile"] )


#
# Add all trxs in mintTrxs to thinmintTrxs if it doesn't already exist.
#
# @param mintTrxs array of trxs data downloaded from mint (doGetTransactions)
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
def forEachTransaction( trxs, func ):
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
# @return formatted trx summary in plain text
#
def formatNewTranText( trx ):
    return "{} {}: {} - {} {}{} {}".format( trx["date"], 
                                         trx["fi"], 
                                         trx["account"], 
                                         trx["merchant"],
                                         "" if trx["isDebit"] else "+",
                                         trx["amount"],
                                         "(pending)" if trx["isPending"] else "" )

#
# @return formatted trx summary in html
#
def formatNewTranHtml( trx ):
    return "<tr><td>{}</td><td>{}: {}</td><td>{}</td><td>{}{}</td><td>{}</td></tr>".format( trx["date"], 
                                                                                            trx["fi"], 
                                                                                            trx["account"], 
                                                                                            trx["merchant"],
                                                                                            "" if trx["isDebit"] else "+",
                                                                                            trx["amount"],
                                                                                            "(pending)" if trx["isPending"] else "" )


#
# @return lines[] of formatted trx data 
#
def formatNewTrans( newTrxs, formatFunc ):
    retMe = []
    for trxId in newTrxs:
        retMe.append( formatFunc( newTrxs[ trxId ] ) )
    return retMe



#
# @return formatted account summary
#
def formatAccountText( act ):
    return "{}: {} - {}".format( act["fiName"],
                                 act["accountName"],
                                 locale.currency( act["currentBalance"] ) )

#
# @return formatted account summary
#
def formatAccountHtml( act ):
    return "<tr><td>{}: {}</td><td>{}</td></tr>".format( act["fiName"],
                                                         act["accountName"],
                                                         locale.currency( act["currentBalance"] ) )


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
# Compose an email summary and write it to the --outputfile
#
def doComposeEmailSummary( args ) : 
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
                     pwd=args["--gmailpassword"] )
    sender.send(message)


#
# main entry point ---------------------------------------------------------------------------
# 
args = verifyArgs( parseArgs() , required_args = [ '--action' ] )
print("main: verified args=", args)


if args["--action"] == "getAccounts":
    args = verifyArgs( args , required_args = [ '--email', '--password', '--outputfile' ] )
    doGetAccounts( args )

elif args["--action"] == "getTransactions":
    args = verifyArgs( args , required_args = [ '--email', '--password', '--outputfile' ] )
    doGetTransactions( args )

elif args["--action"] == "readTransactions":
    args = verifyArgs( args , required_args = [ '--inputfile' ] )
    doReadTransactions( args )

elif args["--action"] == "readAccounts":
    args = verifyArgs( args , required_args = [ '--inputfile' ] )
    doReadAccounts( args )

elif args["--action"] == "convertTransactions":
    args = verifyArgs( args , required_args = [ '--inputfile', '--outputfile' ] )
    doConvertTransactions( args )

elif args["--action"] == "mergeTransactions":
    args = verifyArgs( args , required_args = [ '--mintfile', '--inputfile', '--outputfile' ] )
    doMergeTransactions( args )

elif args["--action"] == "setHasBeenAcked":
    args = verifyArgs( args , required_args = [ '--inputfile', '--outputfile' ] )
    writeJson( forEachTransaction( readJson( args[ '--inputfile' ] ), setHasBeenAcked), args[ '--outputfile' ] )

elif args["--action"] == "composeEmailSummary":
    args = verifyArgs( args , required_args = [ '--transfile', '--accountsfile', '--outputfile' ] )
    doComposeEmailSummary( args )

elif args["--action"] == "sendEmailSummary":
    args = verifyArgs( args , required_args = [ '--inputfile', '--gmailuser', '--gmailpassword', '--to' ] )
    sendEmailSummary( args )



else:
    print ( "main: Unrecognized action: " + args["--action" ] )


