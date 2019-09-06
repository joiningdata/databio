source import_sources.sh

# record metadata
$IMP new org.ensembl.gene "Ensembl Gene ID"
$IMP new org.ensembl.transcript "Ensembl Transcript ID"
$IMP new org.ensembl.protein "Ensembl Protein ID"

$IMP urls org.ensembl.gene "https://ensembl.org" "http://www.ensembl.org/id/%s"
$IMP urls org.ensembl.transcript "https://ensembl.org" "http://www.ensembl.org/id/%s"
$IMP urls org.ensembl.protein "https://ensembl.org" "http://www.ensembl.org/id/%s"

$IMP ref org.ensembl.gene ensembl.ris
$IMP ref org.ensembl.transcript ensembl.ris
$IMP ref org.ensembl.protein ensembl.ris



$IMP index org.ensembl.gene ensembl_genes.txt
$IMP index org.ensembl.transcript ensembl_transcripts.txt
$IMP index org.ensembl.protein ensembl_proteins.txt

### subset indexes
$IMP -s human index org.ensembl.gene hgnc_ensgene.txt
