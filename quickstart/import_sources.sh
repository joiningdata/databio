IMP=../cmd/import/import

$IMP init

$IMP new org.ensembl.gene "Ensembl Gene ID" "https://ensembl.org"
$IMP new org.ensembl.transcript "Ensembl Transcript ID" "https://ensembl.org"
$IMP new org.ensembl.protein "Ensembl Protein ID" "https://ensembl.org"

$IMP -t integer new gov.nih.nlm.ncbi.gene "NCBI Entrez Gene ID" "https://www.ncbi.nlm.nih.gov/gene"

$IMP new org.genenames.gene "HGNC Gene ID" "http://www.genenames.org/"
$IMP new org.genenames.symbol "HGNC Gene Symbol" "http://www.genenames.org/"
$IMP new org.genenames.name "HGNC Gene Name" "http://www.genenames.org/"

## full collection indexes

$IMP index org.ensembl.gene ensembl_genes.txt
$IMP index org.ensembl.transcript ensembl_transcripts.txt
$IMP index org.ensembl.protein ensembl_proteins.txt

$IMP index gov.nih.nlm.ncbi.gene ncbi_genes.txt

$IMP index org.genenames.gene hgnc_ids.txt
$IMP index org.genenames.symbol hgnc_symbols.txt
$IMP index org.genenames.name hgnc_names.txt

############## subsets
$IMP -s human index org.ensembl.gene hgnc_ensgene.txt
#hgnc_ncbigene.txt
#hgnc_refseq.txt

$IMP -s human index gov.nih.nlm.ncbi.gene ncbi_gene.human.txt
$IMP -s mammals index gov.nih.nlm.ncbi.gene ncbi_gene.mammals.txt
$IMP -s plants index gov.nih.nlm.ncbi.gene ncbi_gene.plants.txt

######################################
## identifer mappings

$IMP map gov.nih.nlm.ncbi.gene org.ensembl.gene ncbi_gene2ensembl_gene.tsv
$IMP map gov.nih.nlm.ncbi.gene org.ensembl.transcript ncbi_gene2ensembl_transcript.tsv
$IMP map gov.nih.nlm.ncbi.gene org.ensembl.protein ncbi_gene2ensembl_protein.tsv

$IMP map org.genenames.gene org.genenames.symbol hgnc_id2symbol.tsv
$IMP map org.genenames.gene org.genenames.name hgnc_id2name.tsv
#$IMP map org.genenames.gene xx hgnc_id2refseq.tsv
$IMP map org.genenames.gene gov.nih.nlm.ncbi.gene hgnc_id2ncbigene.tsv
$IMP map org.genenames.gene org.ensembl.gene hgnc_id2ensgene.tsv
