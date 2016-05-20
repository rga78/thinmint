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

if [ -z "$TM_MONGO_URI" ]; then
    usage "\$TM_MONGO_URI is not defined"
fi


#
# Send account refresh signal to mint
# 
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action refreshMintAccounts 
python ./mintclient.py --action refreshMintAccounts 

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
echo python ./mintclient.py --action importMintDataToMongo 
python ./mintclient.py --action importMintDataToMongo 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Sometimes for whatever reason mint makes new copies of tran records.
# This action attempts to transfer the tags from the old "marooned" copies to the new copies
#
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action syncMaroonedTrans 
python ./mintclient.py --action syncMaroonedTrans 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Backfill timeseries data
# Note: this doesn't really have to be run every time.  Most of the time it will just be overwriting
# existing backfill data with the exact same data.  HOWEVER... if a new account shows up in mint, 
# we need to call this to get it backfilled.  AND... if for whatever reason mint deletes or adds an
# old tran to an existing account, we need to call this to update the backfill data.
#
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action backfillAccountsTimeSeries 
python ./mintclient.py --action backfillAccountsTimeSeries 

echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action backfillSummaryTimeSeries 
python ./mintclient.py --action backfillSummaryTimeSeries 


#
# Update account performance (last 7 days, 30 days, and so on)
#
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action setAccountPerformance 
python ./mintclient.py --action setAccountPerformance 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Resolve pending trans
#
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action resolvePendingTransactions 
python ./mintclient.py --action resolvePendingTransactions 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Remove unused tags
#
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action refreshTags 
python ./mintclient.py --action refreshTags 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Auto tag..
#
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action autoTagTrans 
python ./mintclient.py --action autoTagTrans 

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
echo python ./mintclient.py --action syncRemovedPendingTrans 
python ./mintclient.py --action syncRemovedPendingTrans 

if [ $? -ne 0 ]; then
    exit $?
fi


#
# Group tran amounts by tag, by month
#
echo "--------------------------------------------------------------------------------------------"
echo python ./mintclient.py --action groupTransByTagByMonth 
python ./mintclient.py --action groupTransByTagByMonth 

if [ $? -ne 0 ]; then
    exit $?
fi


# -rx- #
# -rx- # Compose email with status update, new trans in need of ACK'ing
# -rx- #
# -rx- echo "--------------------------------------------------------------------------------------------"
# -rx- echo python ./mintclient.py --action composeEmailSummary --outputfile=data/email.txt
# -rx- python ./mintclient.py --action composeEmailSummary --outputfile=data/email.txt
# -rx- 
# -rx- if [ $? -ne 0 ]; then
# -rx-     exit $?
# -rx- fi

# 
# Send email
#
# -rx- echo python ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to robertgalderman@gmail.com --gmailuser xxx --gmailpass xxx
# -rx- python ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'robertgalderman@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"
# -rx- 
# -rx- echo python ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to ilana.bram@gmail.com --gmailuser xxx --gmailpass xxx 
# -rx- python ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'ilana.bram@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"
#
# add user
# ./mintclient.py --action addUser --user xx --pass xx --mintuser xx --mintpass xx


#
# Done
#
echo "--------------------------------------------------------------------------------------------"
echo "Processing complete."


