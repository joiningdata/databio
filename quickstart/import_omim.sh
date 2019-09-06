
source import_sources.sh

# record metadata
$IMP -t integer new org.omim.gene "OMIM Gene ID"
$IMP urls org.omim.gene "https://omim.org" "http://omim.org/entry/%s"
$IMP ref org.omim.gene omim.ris

#############################################
# download and extract the current data

STAMP=`TZ=UTC date "+%FT%T"`
curl -LO https://omim.org/static/omim/data/mim2gene.txt

grep '	gene	' mim2gene.txt|cut -f 1 >omim_genes.txt
grep 'gene	[0-9]' mim2gene.txt|cut -f 1,3 >omim_gene2entrez.tsv
grep 'gene	' mim2gene.txt|cut -f 1,5 |grep -v '\t$' >omim_gene2ensembl.tsv

#############################################

# load the index data and source mappings
$IMP -d $STAMP index org.omim.gene omim_genes.txt

$IMP -d $STAMP map org.omim.gene gov.nih.nlm.ncbi.gene omim_gene2entrez.txt
$IMP -d $STAMP map org.omim.gene org.ensembl.gene omim_gene2ensembl.txt
