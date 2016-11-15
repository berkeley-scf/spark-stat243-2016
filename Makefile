all: spark.html spark_slides.html 

spark.html: spark.md
	pandoc -s -o $@ $<

spark_slides.html: spark.md
	pandoc -s --webtex -t slidy -o $@ $<

clean:
	rm -rf *html
