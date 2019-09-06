source import_sources.sh

# record metadata
$IMP new org.genenames.gene "HGNC Gene ID"
$IMP new org.genenames.symbol "HGNC Gene Symbol"
$IMP new org.genenames.name "HGNC Gene Name"

$IMP urls org.genenames.gene "http://www.genenames.org/" "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=%s"
$IMP urls org.genenames.symbol "http://www.genenames.org/" "https://www.genenames.org/tools/search/#!/all?query=%s"
$IMP urls org.genenames.name "http://www.genenames.org/" "https://www.genenames.org/tools/search/#!/all?query=%s"

$IMP ref org.genenames.gene hgnc.ris
$IMP ref org.genenames.symbol hgnc.ris
$IMP ref org.genenames.name hgnc.ris

#############################################
# download and extract the current data

STAMP=`TZ=UTC date "+%FT%T"`
curl -o hgnc.tsv 'https://www.genenames.org/cgi-bin/download/custom?col=gd_hgnc_id&col=gd_app_sym&col=gd_app_name&col=gd_pub_refseq_ids&col=gd_pub_eg_id&col=gd_pub_ensembl_id&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit'

cut -f1 hgnc.tsv >hgnc_ids.txt
cut -f2 hgnc.tsv >hgnc_symbols.txt
cut -f3 hgnc.tsv >hgnc_names.txt
#cut -f4 hgnc.tsv >hgnc_refseq.txt
cut -f5 hgnc.tsv >hgnc_ncbigene.txt
cut -f6 hgnc.tsv >hgnc_ensgene.txt

cut -f1,2 hgnc.tsv >hgnc_id2symbol.tsv
cut -f1,3 hgnc.tsv >hgnc_id2name.tsv
#cut -f1,4 hgnc.tsv >hgnc_id2refseq.tsv
cut -f1,5 hgnc.tsv >hgnc_id2ncbigene.tsv
cut -f1,6 hgnc.tsv >hgnc_id2ensgene.tsv

#############################################

# load the index data and source mappings
$IMP -d $STAMP index org.genenames.gene hgnc_ids.txt
$IMP -d $STAMP index org.genenames.symbol hgnc_symbols.txt
$IMP -d $STAMP index org.genenames.name hgnc_names.txt

$IMP -d $STAMP map org.genenames.gene org.genenames.symbol hgnc_id2symbol.tsv
$IMP -d $STAMP map org.genenames.gene org.genenames.name hgnc_id2name.tsv
$IMP -d $STAMP map org.genenames.gene gov.nih.nlm.ncbi.gene hgnc_id2ncbigene.tsv
$IMP -d $STAMP map org.genenames.gene org.ensembl.gene hgnc_id2ensgene.tsv
