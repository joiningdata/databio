export IMP=../cmd/import/import

databio_filestamp() { # [filename.txt]
  ## TODO: this is probably mac/bsd-specific

  ## result should be YYYY-MM-DD'T'HH:MM:SS
  ## in UTC time (no time zone)
  TZ=UTC stat -t "%FT%T" -f "%Sm" $1
}


if [[ ! -e sources.sqlite ]]; then
	$IMP init
fi

#hgnc_ncbigene.txt
#hgnc_refseq.txt

#$IMP map org.genenames.gene xx hgnc_id2refseq.tsv
