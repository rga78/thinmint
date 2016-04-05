#!/bin/sh
#
#


#
# usage
#
usage() {
    bname=`basename $0`
    cat <<HERE 1>&2
Usage: $bname 

Downloads new trans from mint.com.
Merges with existing thinmint trans.
Compose and send email.
HERE
    if [ "$1" ] ; then echo " !! ERROR: $1 " 1>&2; fi
    exit 1
}


if [ "$1" = "-h" ]; then
    usage
fi

source ./.thinmint.env

# -rx- if [ -z "$gmailuser" ]; then
# -rx-     usage "\$gmailuser is not defined"
# -rx- elif [ -z "$gmailpass" ]; then
# -rx-     usage "\$gmailpass is not defined"
if [ -z "$mongouri" ]; then
    usage "\$mongouri is not defined"
fi


#
# Send account refresh signal to mint
# 
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action refreshMintAccounts --mongouri "$mongouri"
./mintclient.py --action refreshMintAccounts --mongouri "$mongouri"

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Sleep for a while. Give mint a chance to refresh its accounts (which could take a while...)
# 
echo "--------------------------------------------------------------------------------------------"
echo "sleep 300: (started `date`)"
sleep 300

#
# Get mint data
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action importMintDataToMongo --mongouri $mongouri
./mintclient.py --action importMintDataToMongo --mongouri "$mongouri"

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Sometimes for whatever reason mint makes new copies of tran records.
# This action attempts to transfer the tags from the old "marooned" copies to the new copies
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action syncMaroonedTrans --mongouri "$mongouri"
./mintclient.py --action syncMaroonedTrans --mongouri "$mongouri"

if [ $? -ne 0 ]; then
    exit $?
fi

# TODO: backfill timeseries
# ./mintclient.py --action backfillAccountsTimeSeries --mongouri $mongouri
# ./mintclient.py --action backfillSummaryTimeSeries --mongouri $mongouri

#
# Update account performance (last 7 days, 30 days, and so on)
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action setAccountPerformance --mongouri $mongouri
./mintclient.py --action setAccountPerformance --mongouri $mongouri

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Resolve pending trans
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action resolvePendingTransactions --mongouri "$mongouri" 
./mintclient.py --action resolvePendingTransactions --mongouri "$mongouri" 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Remove unused tags
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action refreshTags --mongouri "$mongouri" 
./mintclient.py --action refreshTags --mongouri "$mongouri" 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Auto tag..
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action autoTagTrans --mongouri "$mongouri" 
./mintclient.py --action autoTagTrans --mongouri "$mongouri" 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Remove thinmint pending trans that have been removed from mint
#
# Note: we do this after auto-tagging, because sometimes mint creates
#       a new pending tran record for an existing pending tran and
#       removes the old pending tran.  These old pending trans won't
#       be resolved under resolvePendingTransactions (i.e. the tags 
#       from the old pending tran won't be transfered to the new pending
#       tran, since the code only resolves to cleared trans). Best we can 
#       do is let auto-tag copy over the tags from the old pending tran
#       to the new pending tran.
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action syncRemovedPendingTrans --mongouri "$mongouri" 
./mintclient.py --action syncRemovedPendingTrans --mongouri "$mongouri" 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Group tran amounts by tag, by month
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action groupTransByTagByMonth --mongouri "$mongouri" 
./mintclient.py --action groupTransByTagByMonth --mongouri "$mongouri" 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Compose email with status update, new trans in need of ACK'ing
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action composeEmailSummary --mongouri=xxx --outputfile=data/email.txt
./mintclient.py --action composeEmailSummary --mongouri="$mongouri" --outputfile=data/email.txt

if [ $? -ne 0 ]; then
    exit $?
fi

# 
# Send email
#
# -rx- echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to robertgalderman@gmail.com --gmailuser xxx --gmailpass xxx
# -rx- ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'robertgalderman@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"
# -rx- 
# -rx- echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to ilana.bram@gmail.com --gmailuser xxx --gmailpass xxx 
# -rx- ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'ilana.bram@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"
#
# add user
# ./mintclient.py --action addUser --user xx --pass xx --mintuser xx --mintpass xx




