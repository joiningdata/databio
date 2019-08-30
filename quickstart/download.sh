## this will download and extract about 4GB of data for testing

#curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/gene2accession.gz
#curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/gene2refseq.gz

curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/gene2ensembl.gz

gunzip gene2ensembl.gz
cut -f2 gene2ensembl > ncbi_genes.txt
cut -f3 gene2ensembl > ensembl_genes.txt
cut -f5 gene2ensembl > ensembl_transcripts.txt
cut -f7 gene2ensembl > ensembl_proteins.txt

cut -f2,3 gene2ensembl > ncbi_gene2ensembl_gene.tsv
cut -f2,5 gene2ensembl > ncbi_gene2ensembl_transcript.tsv
cut -f2,7 gene2ensembl > ncbi_gene2ensembl_protein.tsv

################

curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/All_Mammalia.gene_info.gz
curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
curl -LO ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Plants/All_Plants.gene_info.gz

# subset files
gzcat All_Plants.gene_info.gz|cut -f2 >ncbi_gene.plants.txt
gzcat All_Mammalia.gene_info.gz|cut -f2 >ncbi_gene.mammals.txt
gzcat Homo_sapiens.gene_info.gz|cut -f2 >ncbi_gene.human.txt

##############3

curl -o hgnc.tsv 'https://www.genenames.org/cgi-bin/download/custom?col=gd_hgnc_id&col=gd_app_sym&col=gd_app_name&col=gd_pub_refseq_ids&col=gd_pub_eg_id&col=gd_pub_ensembl_id&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit'

cut -f1 hgnc.tsv >hgnc_ids.txt
cut -f2 hgnc.tsv >hgnc_symbols.txt
cut -f3 hgnc.tsv >hgnc_names.txt
cut -f4 hgnc.tsv >hgnc_refseq.txt
cut -f5 hgnc.tsv >hgnc_ncbigene.txt
cut -f6 hgnc.tsv >hgnc_ensgene.txt

cut -f1,2 hgnc.tsv >hgnc_id2symbol.tsv
cut -f1,3 hgnc.tsv >hgnc_id2name.tsv
cut -f1,4 hgnc.tsv >hgnc_id2refseq.tsv
cut -f1,5 hgnc.tsv >hgnc_id2ncbigene.tsv
cut -f1,6 hgnc.tsv >hgnc_id2ensgene.tsv
