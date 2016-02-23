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

if [ -z "$mintuser" ]; then
    usage "\$mintuser is not defined"
elif [ -z "$mintpass" ]; then
    usage "\$mintpass is not defined"
elif [ -z "$gmailuser" ]; then
    usage "\$gmailuser is not defined"
elif [ -z "$gmailpass" ]; then
    usage "\$gmailpass is not defined"
elif [ -z "$mongouri" ]; then
    usage "\$mongouri is not defined"
fi

#
# Send account refresh signal to mint
# 
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action refreshMintAccounts --mintuser xxx --mintpass xxx 
./mintclient.py --action refreshMintAccounts --mintuser "$mintuser" --mintpass "$mintpass" 

#
# Sleep for a few seconds to let the mint account refresh above finish
#
echo "--------------------------------------------------------------------------------------------"
echo "sleep 15"
sleep 15

#
# Get mint data
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action importMintDataToMongo --mintuser xxx --mintpass xxx --mongouri $mongouri
./mintclient.py --action importMintDataToMongo --mintuser "$mintuser" --mintpass "$mintpass" --mongouri "$mongouri"

#
# Update account performance (last 7 days, 30 days, and so on)
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action setAccountPerformance --mongouri $mongouri
./mintclient.py --action setAccountPerformance --mongouri $mongouri

#
# Resolve pending trans
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action resolvePendingTransactions --mongouri "$mongouri" 
./mintclient.py --action resolvePendingTransactions --mongouri "$mongouri" 

#
# Remove unused tags
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action refreshTags --mongouri "$mongouri" 
./mintclient.py --action refreshTags --mongouri "$mongouri" 

#
# Auto tag..
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action autoTagTrans --mongouri "$mongouri" 
./mintclient.py --action autoTagTrans --mongouri "$mongouri" 

#
# Compose email with status update, new trans in need of ACK'ing
#
echo "--------------------------------------------------------------------------------------------"
echo ./mintclient.py --action composeEmailSummary --mongouri=xxx --outputfile=data/email.txt
./mintclient.py --action composeEmailSummary --mongouri="$mongouri" --outputfile=data/email.txt

# 
# Send email
#
# -rx- echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to robertgalderman@gmail.com --gmailuser xxx --gmailpass xxx
# -rx- ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'robertgalderman@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"
# -rx- 
# -rx- echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to ilana.bram@gmail.com --gmailuser xxx --gmailpass xxx 
# -rx- ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'ilana.bram@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"




