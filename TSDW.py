import csv

import HeidelTime
import WikiExtractor
from WikiExtractor import *

import sys
import argparse
import logging
import os.path
import re
import shutil

from glob import iglob
from io import StringIO
from multiprocessing import cpu_count
from timeit import default_timer

# Program version
version = '3.2.18'

# ----------------------------------------------------------------------
# addional options

tsdw_options = SimpleNamespace(

    # chosen language
    language='english',

    # maximum value of the artcile id
    maxid=-1,

    # minimum value of the artcile id
    minid=-1
)

# ----------------------------------------------------------------------
# RE patterns

tag_check = re.compile(r'<a href=" *(.*?)">.*?</a>|<TIMEX3 tid=".*?" type="DATE" value="(.*?)"(?: mod=".*?")??>(.*?)</TIMEX3>')
doc_match = re.compile(r'<doc id="(\d*)" revid="(\d*)".*title="([^>]*)">(.*)</doc>', re.S)
doc_end = re.compile(r'(</doc>)')
sen_split = re.compile(r'(?<=[.:;?!])[\n "]|BULLET::::|\n')
make_intervals = re.compile(
    r'<TIMEX3 tid="(?P<tid>[^"]*)" type="DATE" value="(?P<from>[^"]*)">(?P<og1>[^<]*)</TIMEX3> ?– ?<TIMEX3 tid="[^"]*" '
    r'type="DATE" value="(?P<till>[^"]*)">(?P<og2>[^<]*)</TIMEX3>')
stacked_tags = re.compile(
    r'<a href="(?:[^"<]*(<TIMEX3 tid="[^"]*" type="DATE" value="[^"]*">)[^<]*(</TIMEX3>))+[^"]*">')
date_match = re.compile(r'^((?:BC)?\d{4}(?:-\d{2}$)*)\D*', re.M)
decade_match = re.compile(r'^(?:BC)?\d{3}$', re.M)
century_match = re.compile(r'^(?:BC)?\d{2}$', re.M)
interval_match = re.compile(r'^\[(?:BC)?\d{4}(?:-\d{2})*,(?:BC)?\d{4}(?:-\d{2})*]$', re.M)
all_tags = re.compile(r'<TIMEX3 .*?>|</TIMEX3>|<a .*?>|</a>')
cut_article = re.compile(r'<TimeML>\n(.*)</TimeML>', re.S)
section_split = re.compile(r'(?=Section::::)')


class CsvSplitter(object):
    """
    File-like object, that splits output to multiple csv files of a given max size.
    """

    def __init__(self, nextFile, max_file_size=0, compress=True):
        """
        :param nextFile: a NextFile object from which to obtain filenames
            to use.
        :param max_file_size: the maximum size of each file.
        :para compress: whether to write data with bzip compression.
        """
        self.nextFile = nextFile
        self.compress = compress
        self.max_file_size = max_file_size
        self.file = self.open(next(self.nextFile))
        self.writer = csv.writer(self.file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\', quotechar='')

    def reserve(self, size):
        if self.file.tell() + size > self.max_file_size:
            self.close()
            self.file = self.open(next(self.nextFile))
            self.writer = csv.writer(self.file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\', quotechar='')

    def write(self, data):
        self.reserve(len(data))
        self.writer.writerow(data)

    def close(self):
        self.file.close()

    def open(self, filename):
        return open(filename + '.csv', 'w', newline='', encoding='utf-8')


# ----------------------------------------------------------------------
# Wikiextractor Monkey patch

def makeInternalLink(title, label):
    colon = title.find(':')
    if colon > 0 and title[:colon] not in options.acceptedNamespaces:
        return ''
    if colon == 0:
        # drop also :File:
        colon2 = title.find(':', colon + 1)
        if colon2 > 1 and title[colon + 1:colon2] not in options.acceptedNamespaces:
            return ''
    if options.keepLinks:
        return '<a href="%s">%s</a>' % (title, label)
    else:
        return label


def process_dump(custom_input, template_file, out_file, file_size, file_compress,
                 process_count):
    """
    :param custom_input: name of the wikipedia dump file; '-' to read from stdin
    :param template_file: optional file with template definitions.
    :param out_file: directory where to store extracted data, or '-' for stdout
    :param file_size: max size of each extracted file, or None for no max (one file)
    :param file_compress: whether to compress files with bzip.
    :param process_count: number of extraction processes to spawn.
    """
    if custom_input == '-':
        input_file = sys.stdin
    else:
        input_file = fileinput.FileInput(custom_input, openhook=fileinput.hook_compressed)

    # collect siteinfo
    for line in input_file:
        # When an input file is .bz2 or .gz, line can be a bytes even in Python 3.
        if not isinstance(line, text_type):
            line = line.decode('utf-8')
        m = tagRE.search(line)
        if not m:
            continue
        tag = m.group(2)
        if tag == 'base':
            # discover urlbase from the xml dump file
            # /mediawiki/siteinfo/base
            base = m.group(3)
            options.urlbase = base[:base.rfind("/")]
        elif tag == 'namespace':
            mk = keyRE.search(line)
            if mk:
                nsid = ''.join(mk.groups())
            else:
                nsid = ''
            options.knownNamespaces[m.group(3)] = nsid
            if re.search('key="10"', line):
                options.templateNamespace = m.group(3)
                options.templatePrefix = options.templateNamespace + ':'
            elif re.search('key="828"', line):
                options.moduleNamespace = m.group(3)
                options.modulePrefix = options.moduleNamespace + ':'
        elif tag == '/siteinfo':
            break

    if options.expand_templates:
        # preprocess
        template_load_start = default_timer()
        if template_file:
            if os.path.exists(template_file):
                logging.info("Loading template definitions from: %s", template_file)
                # can't use with here:
                file = fileinput.FileInput(template_file,
                                           openhook=fileinput.hook_compressed)
                load_templates(file)
                file.close()
            else:
                if custom_input == '-':
                    # can't scan then reset stdin; must error w/ suggestion to specify template_file
                    raise ValueError("to use templates with stdin dump, must supply explicit template-file")
                logging.info("Preprocessing '%s' to collect template definitions: this may take some time.",
                             custom_input)
                load_templates(input_file, template_file)
                input_file.close()
                input_file = fileinput.FileInput(custom_input, openhook=fileinput.hook_compressed)
        template_load_elapsed = default_timer() - template_load_start
        logging.info("Loaded %d templates in %.1fs", len(options.templates), template_load_elapsed)

    # process pages
    logging.info("Starting page extraction from %s.", custom_input)
    extract_start = default_timer()

    # Parallel Map/Reduce:
    # - pages to be processed are dispatched to workers
    # - a reduce process collects the results, sort them and print them.

    process_count = max(1, process_count)
    maxsize = 1 * process_count
    # output queue
    output_queue = Queue(maxsize=maxsize)

    if out_file == '-':
        out_file = None

    worker_count = process_count

    # load balancing
    max_spool_length = 10000
    spool_length = Value('i', 0, lock=False)

    # reduce job that sorts and prints output
    reduce = Process(target=reduce_process,
                     args=(options, tsdw_options, output_queue, spool_length,
                           out_file, file_size))
    reduce.start()

    # initialize jobs queue
    jobs_queue = Queue(maxsize=maxsize)

    # start worker processes
    logging.info("Using %d extract processes.", worker_count)
    workers = []
    for i in range(worker_count):
        extractor = Process(target=extract_process,
                            args=(options, tsdw_options, jobs_queue, output_queue))
        extractor.daemon = True  # only live while parent process lives
        extractor.start()
        workers.append(extractor)

    # Generator for already time tagged articles
    gen = load_articles(out_file)
    gen_closed = False

    # Mapper process
    page_num = 0
    for page_data in pages_from(input_file):
        artid, revid, title, ns, catSet, page = page_data
        if keepPage(ns, catSet, page) and int(artid) >= tsdw_options.minid:
            if int(artid) > tsdw_options.maxid != -1:
                break
            # slow down
            delay = 0
            if spool_length.value > max_spool_length:
                # reduce to 10%
                while spool_length.value > max_spool_length / 10:
                    time.sleep(10)
                    delay += 10
            if delay:
                logging.info('Delay %ds', delay)

            # compare allready processed articles and mark them to be skipped if they are a match
            if not gen_closed:
                skip = False  # reset the flag
                try:
                    pre = next(gen)
                    # load next article until the id of the reloaded article
                    # is greater than the one of the xml file
                    while pre and pre[0] < artid:
                        pre = next(gen)

                    # only use the reloaded article if id and revid match
                    if pre and pre[0] == artid and pre[1] == revid:
                        artid, revid, title, page = pre
                        skip = True

                except StopIteration:
                    # mark that the generator reached the end
                    gen_closed = True

            job = (artid, revid, title, page, page_num, skip)
            jobs_queue.put(job)  # goes to any available extract_process
            page_num += 1
        page = None  # free memory
    gen.close()
    input_file.close()

    # signal termination
    for _ in workers:
        jobs_queue.put(None)
    # wait for workers to terminate
    for w in workers:
        w.join()

    # signal end of work to reduce process
    output_queue.put(None)
    # wait for it to finish
    reduce.join()

    finalize_output(out_file)

    extract_duration = default_timer() - extract_start
    extract_rate = page_num / extract_duration
    logging.info("Finished %d-process extraction of %d articles in %.1fs (%.1f art/s)",
                 process_count, page_num, extract_duration, extract_rate)


def extract_process(opts, add_opts, jobs_queue, output_queue):
    """Pull tuples of raw page content, do CPU/regex-heavy fixup, push finished text
    :param opts: options
    :param i: process id.
    :param jobs_queue: where to get jobs.
    :param output_queue: where to queue extracted text for output.
    """
    global options
    options = opts
    global tsdw_options
    tsdw_options = add_opts
    global document_creation_date

    # initialize heideltime
    hw = HeidelTime.HeidelTimeWrapper(tsdw_options.language)

    createLogger(options.quiet, options.debug, options.log_file)
    out = StringIO()  # memory buffer

    while True:
        job = jobs_queue.get()  # job is (id, revid, title, page, page_num, skip_flag)
        if job:
            id, revid, title, page, page_num, skip = job
            t_text = ''
            try:
                e = Extractor(*job[:4])  # (id, revid, title, page)
                if skip:
                    # Skipping Heideltime and Wikiextractor processing
                    page = page.split('\n')
                    e.write_output(out, page[1:-2])
                    text = out.getvalue()
                else:
                    # Normal processing of the text
                    e.extract(out)
                    text = out.getvalue()  # get normal text by wikiextractor
                    text = re.sub(r'BULLET::::(.*)\n', r'\g<1>.\n', text)

                    # split the articles into sections
                    sections = section_split.split(text)
                    text = ''
                    for section in sections:
                        text += cut_article.findall(hw.parse(section.encode('utf-8')))[0]

                    # remove intertwined tags
                    text = stacked_tags.sub('', text)

                page = None  # free memory
            except:
                text = ''
                logging.exception('Processing page: %s %s', id, title)

            sid = 0
            page_res = list()

            # split into sentences and process them
            for sentence in sen_split.split(text):
                if len(sentence) > 3:
                    page_res.extend(process_sentence(sid, sentence))
                    sid += 1

            # put results into queue for the reduce process
            output_queue.put((id, page_num, page_res, title, text))
            out.truncate(0)
            out.seek(0)
        else:
            logging.debug('Quit extractor')
            break
    out.close()


def reduce_process(opts, add_opts, output_queue, spool_length,
                   out_file=None, file_size=0):
    """Pull finished article text, write series of files (or stdout)
    :param add_opts: additional options
    :param opts: global parameters.
    :param output_queue: text to be output.
    :param spool_length: spool length.
    :param out_file: filename where to print.
    :param file_size: max file size.
    :param file_compress: whether to compress output.
    """
    global options
    options = opts
    global tsdw_options
    tsdw_options = add_opts

    createLogger(options.quiet, options.debug, options.log_file)

    # set up files for output
    if out_file:
        nextFile_1 = NextFile(out_file + "/tmp")
        nextFile_2 = NextFile(out_file + "/data")
        output_1 = OutputSplitter(nextFile_1, file_size, False)
        output_2 = CsvSplitter(nextFile_2, file_size, False)
    else:
        logging.ERROR("Couldn't determine an output path")

    interval_start = default_timer()

    spool = {}  # collected pages
    next_page = 0  # sequence numbering of

    while True:
        if next_page in spool:
            id, page_num, page_res, title, text = spool.pop(next_page)

            # write output
            output_1.write(text.encode('utf-8'))
            for line in format_pairs(id, page_res):
                output_2.write(line)
            logging.info('%s %s', id, title)
            next_page += 1

            # tell mapper our load:
            spool_length.value = len(spool)
            # progress report
            if next_page % report_period == 0:
                interval_rate = report_period / (default_timer() - interval_start)
                logging.info("Extracted %d articles (%.1f art/s)",
                             next_page, interval_rate)
                interval_start = default_timer()
        else:
            # mapper puts None to signal finish
            next = output_queue.get()
            if not next:
                break
            page_num = next[1]
            spool[page_num] = next
            # tell mapper our load:
            spool_length.value = len(spool)
            # FIXME: if an extractor dies, process stalls; the other processes
            # continue to produce pairs, filling up memory.
            if len(spool) > 200:
                logging.debug('Collected %d, waiting: %d, %d', len(spool),
                              next_page, next_page == page_num)

    output_1.close()
    output_2.close()


# complete monkey patch
WikiExtractor.makeInternalLink = makeInternalLink
WikiExtractor.process_dump = process_dump
WikiExtractor.reduce_process = reduce_process
WikiExtractor.extract_process = extract_process


# ----------------------------------------------------------------------
# TSDW methods

'''
def regex_ttag(text):
    """ tags temporal exprssions
    :param text: clean text
    :return: temporal tagged text
    """
    #FIXME: centuries are returned 1 too high
    text = re.sub(r'(?<=\D)(?P<og>\d{4}(?=[^\w\d])(?! ?B.?C.?E?))', r'<TIMEX3 tid="t0" type="DATE" value="\g<og>">\g<og></TIMEX3>', text)
    text = re.sub(r'(?<=\D)(?P<og>(?P<num>\d{3}) ?A.?D.?)', r'<TIMEX3 tid="t0" type="DATE" value="0\g<num>">\g<og></TIMEX3>', text)
    text = re.sub(r'(?<=\D)(?P<og>(?P<num>\d{2}) ?A.?D.?)', r'<TIMEX3 tid="t0" type="DATE" value="00\g<num>">\g<og></TIMEX3>', text)
    text = re.sub(r'(?<=\D)(?P<og>(?P<num>\d) ?A.?D.?)', r'<TIMEX3 tid="t0" type="DATE" value="000\g<num>">\g<og></TIMEX3>', text)
    text = re.sub(r'(?<=\D)(?P<og>(?P<num>\d{4}) ?B.?C.?E?)', r'<TIMEX3 tid="t0" type="DATE" value="BC\g<num>">\g<og></TIMEX3>', text)
    text = re.sub(r'(?<=\D)(?P<og>(?P<num>\d{3})0s)(?=[^\w\d])', '<TIMEX3 tid="t0" type="DATE" value="\g<num>">\g<og></TIMEX3>', text)
    text = re.sub(r'(?<=\D)(?P<og>(?P<num>\d{2})(?:st|nd|rd|th) century)(?=[^\w\d])(?! ?B.?C.?E?)', '<TIMEX3 tid="t0" type="DATE" value="\g<num>">\g<og></TIMEX3>', text)
    text = re.sub(r'(?<=\D)(?P<og>(?P<num>\d{2})(?:st|nd|rd|th) century ?B.?C.?E?)', '<TIMEX3 tid="t0" type="DATE" value="\g<num>">\g<og></TIMEX3>', text)
    return text
'''


def process_sentence(sid, text):
    """ takes a sentence and sentence id and combines contained tagged dates and link
    :param sid: sentence id.
    :param text: sentence
    :return: yields resulting (sid, link, [date1, date2, ..., datex])
    """
    clean = all_tags.sub('', text)
    text = make_intervals.sub(r'<TIMEX3 tid="\g<tid>" type="DATE" value="[\g<from>,\g<till>]">\g<og1>-\g<og2></TIMEX3>',
                              text)
    current_title = None
    dates = list()
    # retrieve all dates and links in the sentence in correct order
    for tag in tag_check.findall(text):
        if tag[0]:
            if current_title and dates:
                yield ((sid, current_title, dates.copy(), clean))
            dates.clear()
            current_title = tag[0]
        else:
            date = filter_year(tag[1])
            if date:
                # collect dates that appear after the current link
                dates.append(date)
    if current_title and dates:
        yield ((sid, current_title, dates.copy(), clean))


def filter_year(text):
    """filters needed dates from the input
    wanted tags: 'YY', 'YYY', 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', intervals, optional with preceding BC
    unwanted tags: Any incomplete Reference like 'XXXX-MM-DD', 'PRESENT_REF', 'FUTURE_REF', 'PAST_REF', UNDEF_REF
    changes: Seasons and any other annotations are dropped, century and decade are labeled
    :param text: Date reference in the TIMEX3 standard
    :return:
    """
    # dates
    date = date_match.findall(text)
    if date:
        return date[0]
    # decades
    date = decade_match.findall(text)
    if date:
        return 'DECADE ' + date[0]
    # centuries
    date = century_match.findall(text)
    if date:
        return 'CENTURY ' + date[0]
    # intervals
    date = interval_match.findall(text)
    if date:
        return date[0]
    return


def format_pairs(artid, page_res):
    """ transforms extracted results into an output line, to be saved in a csv file
    :param job: contains article id, revision id, page number, page results, article title
    :return:['artid=__', 'senid=__', 'link=__', 'dates=[date1, date2, ..., datex]', 'sentence=__']
    """
    for sen_res in page_res:
        dates = ''
        for element in sen_res[2]:
            dates = dates + element + ','
        dates = '[' + dates[:-1] + ']'
        yield [f'artid={artid}', f'senid={sen_res[0]}', f'link={sen_res[1]}', f'dates={dates}', f'sentence={sen_res[3]}']


def load_articles(path):
    """load articles from file system with structure path/**/*
    containing files in utf-8 format
    :param path: file path with subfolders containing the target files
    :return: yielding one article at a time
    """
    file_list = [f for f in iglob(path + '\\text/**/*') if os.path.isfile(f)]
    for filename in file_list:
        try:
            with open(filename, encoding='utf8') as currentfile:
                text = ''
                for line in currentfile:
                    # collect the page until document end is found
                    text += line
                    if doc_end.findall(line):
                        art = doc_match.findall(text)[0]
                        text = ''  # reset for next article
                        yield art  # yield found article
            currentfile.close()
        except IOError:
            print('error')


def finalize_output(output_file):
    """after successful finishing the computation, the old data set in 'output_file'/text
    will be replaced by the new results in 'output_file'/tmp
    :param output_file: choseen output folder (default = out)
    """
    # delete old folder
    if os.path.isdir(output_file + '/text'):
        shutil.rmtree(output_file + '/text')

    # rename new folder
    os.rename(output_file + '/tmp', output_file + '/text')


# ----------------------------------------------------------------------


def main():
    """altered Wikiextractor main method
    added additional arguments
    auto-handel other arguments
    - handaling arguments
    - reading xml and saving tuples
    - reading and processing tuples
    - saving vectors
    """
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input",
                        help="XML wiki dump file")

    groupO = parser.add_argument_group('Output')
    groupO.add_argument("-o", "--output", default="out",
                        help="directory for extracted files (or '-' for dumping to stdout)")

    groupP = parser.add_argument_group('Processing')
    default_process_count = max(1, cpu_count() - 1)
    groupP.add_argument("-p", "--processes", type=int, default=default_process_count,
                        help="Number of processes to use (default=%(default)s)")
    groupP.add_argument("-min", "--min_artid", type=int, default=-1,
                        help="minimal id of the articles to be extracted")
    groupP.add_argument("-max", "--max_artid", type=int, default=-1,
                        help="maximum id of the articles to be extracted")
    groupP.add_argument("-lang", "--language", default='english',
                        help="language of the input (default=%(default)")

    groupS = parser.add_argument_group('Special')
    groupS.add_argument("-q", "--quiet", action="store_true",
                        help="suppress reporting progress info")
    groupS.add_argument("--debug", action="store_true",
                        help="print debug info")
    groupS.add_argument("--log_file",
                        help="path to save the log info")
    groupS.add_argument("-v", "--version", action="version",
                        version='%(prog)s ' + version,
                        help="print program version")
    groupP.add_argument("--filter_category",
                        help="specify the file that listing the Categories you want to include or exclude. One line for"
                             "one category. starting with: 1) '#' comment, ignored; 2) '^' exclude; Note: excluding "
                             "has higher priority than including")
    args = parser.parse_args()

    options.keepLinks = True
    options.keepSections = False
    options.keepLists = True
    options.toHTML = False
    options.write_json = False
    options.print_revision = True
    options.min_text_length = 0
    options.expand_templates = False
    options.filter_disambig_pages = False
    options.keep_tables = False

    tsdw_options.maxid = args.max_artid
    tsdw_options.minid = args.min_artid
    tsdw_options.language = args.language

    ignoredTags = [
        'abbr', 'b', 'big', 'blockquote', 'center', 'cite', 'em',
        'font', 'h1', 'h2', 'h3', 'h4', 'hiero', 'i', 'kbd',
        'p', 'plaintext', 's', 'span', 'strike', 'strong',
        'tt', 'u', 'var'
    ]

    for tag in ignoredTags:
        ignoreTag(tag)

    FORMAT = '%(levelname)s: %(message)s'
    logging.basicConfig(format=FORMAT)

    options.quiet = args.quiet
    options.debug = args.debug
    options.log_file = args.log_file
    createLogger(options.quiet, options.debug, options.log_file)

    input_file = args.input
    output_path = args.output
    if output_path != '-' and not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except:
            logging.error('Could not create: %s', output_path)
            return

    filter_category = args.filter_category
    if filter_category is not None and len(filter_category) > 0:
        with open(filter_category) as f:
            error_cnt = 0
            for line in f.readlines():
                try:
                    line = str(line.strip())
                    if line.startswith('#') or len(line) == 0:
                        continue
                    elif line.startswith('^'):
                        options.filter_category_exclude.add(line.lstrip('^'))
                    else:
                        options.filter_category_include.add(line)
                except Exception as exc:
                    error_cnt += 1
                    print(u"Category not in utf8, ignored. error cnt %d:\t%s" % (error_cnt, exc))
                    print(line)
            logging.info("Excluding categories:", )
            logging.info(str(options.filter_category_exclude))
            logging.info("Including categories:")
            logging.info(str(len(options.filter_category_include)))
    process_dump(input_file, False, output_path, 2 ** 20,
                 False, args.processes)


if __name__ == '__main__':
    main()
