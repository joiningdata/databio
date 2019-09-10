source import_sources.sh

# record metadata
$IMP -t integer new gov.nih.nlm.ncbi.gene "NCBI Entrez Gene ID"
$IMP urls gov.nih.nlm.ncbi.gene "https://www.ncbi.nlm.nih.gov/gene" "https://www.ncbi.nlm.nih.gov/gene/%s"
$IMP ref gov.nih.nlm.ncbi.gene entrez.ris

#############################################
# download and extract the current data

STAMP=`TZ=UTC date "+%FT%T"`
curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/gene2ensembl.gz
curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/All_Mammalia.gene_info.gz
curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Plants/All_Plants.gene_info.gz

rm -f gene2ensembl
gunzip gene2ensembl.gz
cut -f2 gene2ensembl > ncbi_genes.txt
cut -f3 gene2ensembl > ensembl_genes.txt
cut -f5 gene2ensembl > ensembl_transcripts.txt
cut -f7 gene2ensembl > ensembl_proteins.txt

cut -f2,3 gene2ensembl > ncbi_gene2ensembl_gene.tsv
cut -f2,5 gene2ensembl > ncbi_gene2ensembl_transcript.tsv
cut -f2,7 gene2ensembl > ncbi_gene2ensembl_protein.tsv

# create subset files
gunzip -c All_Plants.gene_info.gz|cut -f2 >ncbi_gene.plants.txt
gunzip -c All_Mammalia.gene_info.gz|cut -f2 >ncbi_gene.mammals.txt
gunzip -c Homo_sapiens.gene_info.gz|cut -f2 >ncbi_gene.human.txt

##############################################

# load the index data (w/ subsets) and source mappings
$IMP -d $STAMP            index gov.nih.nlm.ncbi.gene ncbi_genes.txt
$IMP -d $STAMP -s human   index gov.nih.nlm.ncbi.gene ncbi_gene.human.txt
$IMP -d $STAMP -s mammals index gov.nih.nlm.ncbi.gene ncbi_gene.mammals.txt
$IMP -d $STAMP -s plants  index gov.nih.nlm.ncbi.gene ncbi_gene.plants.txt

$IMP -d $STAMP map gov.nih.nlm.ncbi.gene org.ensembl.gene ncbi_gene2ensembl_gene.tsv
$IMP -d $STAMP map gov.nih.nlm.ncbi.gene org.ensembl.transcript ncbi_gene2ensembl_transcript.tsv
$IMP -d $STAMP map gov.nih.nlm.ncbi.gene org.ensembl.protein ncbi_gene2ensembl_protein.tsv
