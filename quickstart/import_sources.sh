IMP=../cmd/import/import

$IMP init

$IMP new org.ensembl.gene "Ensembl Gene ID"
$IMP new org.ensembl.transcript "Ensembl Transcript ID"
$IMP new org.ensembl.protein "Ensembl Protein ID"

$IMP -t integer new gov.nih.nlm.ncbi.gene "NCBI Entrez Gene ID"

$IMP new org.genenames.gene "HGNC Gene ID"
$IMP new org.genenames.symbol "HGNC Gene Symbol"
$IMP new org.genenames.name "HGNC Gene Name"

# add in main site, and id-linkout URLs

$IMP urls org.ensembl.gene "https://ensembl.org" "http://www.ensembl.org/id/%s"
$IMP urls org.ensembl.transcript "https://ensembl.org" "http://www.ensembl.org/id/%s"
$IMP urls org.ensembl.protein "https://ensembl.org" "http://www.ensembl.org/id/%s"

$IMP urls gov.nih.nlm.ncbi.gene "https://www.ncbi.nlm.nih.gov/gene" "https://www.ncbi.nlm.nih.gov/gene/%s"

$IMP urls org.genenames.gene "http://www.genenames.org/" "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=%s"
$IMP urls org.genenames.symbol "http://www.genenames.org/" "https://www.genenames.org/tools/search/#!/all?query=%s"
$IMP urls org.genenames.name "http://www.genenames.org/" "https://www.genenames.org/tools/search/#!/all?query=%s"

# add in citations for the resources

$IMP ref org.ensembl.gene ensembl.ris
$IMP ref org.ensembl.transcript ensembl.ris
$IMP ref org.ensembl.protein ensembl.ris

$IMP ref gov.nih.nlm.ncbi.gene entrez.ris

$IMP ref org.genenames.gene hgnc.ris
$IMP ref org.genenames.symbol hgnc.ris
$IMP ref org.genenames.name hgnc.ris

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
