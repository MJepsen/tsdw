# TSDW
TDSW is a Python script for multilingual extraction of corresponding dates and Wikipedia articles from a Wikipedia database dump. The article extraction is performed by the [WikiExtractor.py](http://medialab.di.unipi.it/wiki/Wikipedia_Extractor) and the temporal tagging by [HeidelTime](https://github.com/HeidelTime/heideltime).

## Requirements
The requirements are Python 3.3+, Java 11+, JPype1 and numpy, as well as a [Wikipedia database dump](http://download.wikimedia.org/).

HeidelTime requires the TreeTagger for sentence, token, and part-of speech annotations.
The TreeTagger is included for Windows64 versions. If any other system is used, or compatibility issues appear, please follow the instructions on this website and install the correct version into the TreeTagger folder:
    
    http://www.cis.uni-muenchen.de/~schmid/tools/TreeTagger/

If the TreeTagger is installed elsewhere, please correct the path in the config.props file.
 
JPype1 and numpy can be installed using:
    
    pip install -r requirements.txt


The newest english wikipedia dump file can be found at:

    http://dumps.wikimedia.org/XXwiki/latest/XXwiki-latest-pages-articles.XML.bz2
    
with XX being the language identifier (e.g. en, es, de).

## Usage
The script is invoked with a Wikipedia dump file as argument. The output is stored in multiple CSV files in the folder '/out/data' unless specified otherwise.
The CSV files will contain extracted dates and articles in each line of the following format:

    artid=__;senid=__;link=__;dates=__;sentence=__

artid: id of the article,
senid: id of the sentence containing the detected pair,
link: the title of the article involved,
dates: all associated dates as a list,
sentence: the original corresponding sentence

The files in 'out/text' are extraction templates to speed up extraction, in case the script is used again.

    TSDW.py [-h] [-o OUTPUT] [-p PROCESSES] [-min MINID] [-max MAXID]
        [-lang LANGUAGE] [-f path_of_categories_file] [--debug] [-q]
        [-v] [--log_file]
        input

    positional arguments:
      input                          XML wiki dump file

    optional arguments:
      -h, --help                     show help message
      -p, --processes PROCESSES      Number of processes to use
	
    Output:
      -o, --output OUTPUT            directory for output files

    Processing:
      -min, --min_artid              only extract articles with this id or higher
      -max, --max_artid              only extract articles with this id or lower
      -lang, --language              set to the language of the input (default=english).
                                     compatible with: english, german, dutch, italian, spanish, 
                                     arabic, french, chinese, russian, portuguese
      -f, --filter_category path_of_categories_file
                                     *adopted from Wikiextractor*
                                     Include or exclude specific categories from the dataset.
                                     Specify the categories in file 'path_of_categories_file'.
                                     Format:
                                     One category one line, and if the line
                                     starts with:
                                         1) #: Comments, ignored;
                                         2) ^: the categories will be in excluding-categories
                                         3) others: the categories will be in including-categories.
                                     Priority:
                                         1) If excluding-categories is not empty, and any category of a page
                                            exists in excluding-categories, the page will be excluded; else
                                         2) If including-categories is not empty, and no category of a page
                                            exists in including-categories, the page will be excluded; else
                                         3) the page will be included

    Special:
      -q, --quiet                    suppress progress info
      --debug                        show debug info
      -v, --version                  print version of TSDW
      --log_file                     specify a file for log information.