tsdw.py [-h] [-o OUTPUT] [-p PROCESSES] [-min MINID] [-max MAXID] [-lang LANGUAGE]
			[-f path_of_categories_file] [--debug] [-q] [-v] [--log_file]
			input


    positional arguments:
      input                 		XML wiki dump file

    optional arguments:
      -h, --help            		show this help message and exit
      -p, --processes PROCESSES		Number of processes to use (default 1)
	
    Output:
      -o, --output OUTPUT		    directory for extracted files

    Processing:
      -min, --min_artid             only extract articles with this id or higher
      -max, --max_artid		        only extract articles with this id or lower
      -lang, --language             set to the language of the input (default=english).
                                    compatible with: english, german, dutch, italian, spanish, arabic, french, chinese, russian, portuguese
      -f, --filter_category path_of_categories_file
                            		Include or exclude specific categories from the dataset. Specify the categories in
                            		file 'path_of_categories_file'. Format:
                            		One category one line, and if the line starts with:
                                		1) #: Comments, ignored;
                                		2) ^: the categories will be in excluding-categories
                                		3) others: the categories will be in including-categories.
                            		Priority:
                                		1) If excluding-categories is not empty, and any category of a page exists in excluding-categories, the page will be excluded; else
                                		2) If including-categories is not empty, and no category of a page exists in including-categories, the page will be excluded; else
                                		3) the page will be included

    Special:
      -q, --quiet           		suppress reporting progress info
      --debug               		print debug info
      -v, --version			        print version
      --log_file            		specify a file to save the log information.
