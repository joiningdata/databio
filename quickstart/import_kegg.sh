
source import_sources.sh

#### get list of organism codes from here:
## http://rest.kegg.jp/list/organism

# record metadata
$IMP new jp.kegg.gene "KEGG Gene ID"
$IMP urls jp.kegg.gene "https://kegg.jp" "https://www.genome.jp/dbget-bin/www_bget?%s"
$IMP ref jp.kegg.gene kegg.ris

# record metadata
$IMP new org.uniprot.acc "UniprotKB Accession"
$IMP urls org.uniprot.acc "https://www.uniprot.org" "https://www.uniprot.org/uniprot/%s"
$IMP ref org.uniprot.acc uniprot.ris

#############################################
# download and extract the current data

STAMP=`TZ=UTC date "+%FT%T"`
echo "entrez_gene_id	kegg_gene_id" >ncbi_gene2kegg_genes.tsv
echo "uniprot_id	kegg_gene_id" >uniprot2kegg_genes.tsv

for org in aga ath bta cel cfa dme dre eco ecs gga hsa mmu mcc pfa ptr rno sce ssc xla; do
    echo "kegg_gene_id" > kegg_genes_${org}.txt
    curl -L http://rest.kegg.jp/list/$org |cut -f 1 >> kegg_genes_${org}.txt
    curl -L http://rest.kegg.jp/conv/$org/ncbi-geneid | sed 's/ncbi-geneid://' >> ncbi_gene2kegg_genes.tsv
    curl -L http://rest.kegg.jp/conv/$org/uniprot | sed 's/up://' >> uniprot2kegg_genes.tsv
	sleep 1
done

cut -f1 uniprot2kegg_genes.tsv >uniprot_accessions.txt

# give slightly friendlier names than the 3-letter codes
$IMP -d $STAMP -s "Anopheles"  index jp.kegg.gene kegg_genes_aga.txt
$IMP -d $STAMP -s "Arabidopsis"  index jp.kegg.gene kegg_genes_ath.txt
$IMP -d $STAMP -s "Bovine"  index jp.kegg.gene kegg_genes_bta.txt
$IMP -d $STAMP -s "Worm"  index jp.kegg.gene kegg_genes_cel.txt
$IMP -d $STAMP -s "Canine"  index jp.kegg.gene kegg_genes_cfa.txt
$IMP -d $STAMP -s "Fly"  index jp.kegg.gene kegg_genes_dme.txt
$IMP -d $STAMP -s "Zebrafish"  index jp.kegg.gene kegg_genes_dre.txt
$IMP -d $STAMP -s "E coli strain K12"  index jp.kegg.gene kegg_genes_eco.txt
$IMP -d $STAMP -s "E coli strain Sakai"  index jp.kegg.gene kegg_genes_ecs.txt
$IMP -d $STAMP -s "Chicken"  index jp.kegg.gene kegg_genes_gga.txt
$IMP -d $STAMP -s "Human"  index jp.kegg.gene kegg_genes_hsa.txt
$IMP -d $STAMP -s "Mouse"  index jp.kegg.gene kegg_genes_mmu.txt
$IMP -d $STAMP -s "Rhesus"  index jp.kegg.gene kegg_genes_mcc.txt
$IMP -d $STAMP -s "Malaria"  index jp.kegg.gene kegg_genes_pfa.txt
$IMP -d $STAMP -s "Chimp"  index jp.kegg.gene kegg_genes_ptr.txt
$IMP -d $STAMP -s "Rat"  index jp.kegg.gene kegg_genes_rno.txt
$IMP -d $STAMP -s "Yeast"  index jp.kegg.gene kegg_genes_sce.txt
$IMP -d $STAMP -s "Pig"  index jp.kegg.gene kegg_genes_ssc.txt
$IMP -d $STAMP -s "Xenopus"  index jp.kegg.gene kegg_genes_xla.txt

$IMP -d $STAMP index org.uniprot.acc uniprot_accessions.txt

$IMP -d $STAMP map gov.nih.nlm.ncbi.gene jp.kegg.gene ncbi_gene2kegg_genes.tsv
$IMP -d $STAMP map org.uniprot.acc jp.kegg.gene uniprot2kegg_genes.tsv