__author__ = 'saikaushik.mallela'

from elasticsearch import Elasticsearch
from urllib.parse import urlparse
from urllib.parse import urlunparse
from urllib.parse import urljoin
import urllib.robotparser
from pprint import pprint
import time
import json
import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import socket

es = Elasticsearch("localhost:9200",timeout = 600,max_retries = 2,revival_delay = 0)
robots_txt = "robots.txt"

min_no_of_docs = 20000

seeds =[
        'http://en.wikipedia.org/wiki/Climate_change',
        'https://www.google.com/search?client=safari&rls=en&q=CLIMATE+CHANGE&ie=UTF-8&oe=UTF-8#q=CLIMATE+CHANGE&safe=off&rls=en&tbm=nws',
        'http://en.wikipedia.org/wiki/Effects_of_climate_change_on_humans',
        'http://en.wikipedia.org/wiki/Economic_impacts_of_climate_change']

not_allowed = ['.jpg', '.JPG', '.png', '.PNG', '.ogg', 'mp4', '.tif', '.tiff', '.gif', '.jpeg', '.jif', '.jfif',
               '.jp2', '.jpx', '.j2k', '.j2c', 'fpx', '.pcd', '.pdf']

crawldct ={}
crawled_urls = {}
child_links_yet_to_be_crawled = set()
uncrawled_inlink_map  ={}
file_dct = {}


def get_contents_of_page(soup):
    for script in soup(["script", "style", "noscript", "nav", "footer", "title"]):
        script.extract()    # rip it out

    nav = soup.find('div', id="jump-to-nav")
    if nav:
        nav.extract()

    share = soup.find('div', id="shareFooterSub")
    if share:
        share.extract()

    site_nav = soup.find('div', id="siteNavCategories")
    if site_nav:
        site_nav.extract()

    site_nav = soup.find('div', id="siteNavMore")
    if site_nav:
        site_nav.extract()

    tablet = soup.find('div', id="portraitMess")
    if tablet:
        tablet.extract()

    share_box = soup.find('div', id="shareBox")
    if share_box:
        share_box.extract()

    site_head = soup.find('header', id="siteHead")
    if site_head:
        site_head.extract()

    footer = soup.find('div', id="footer")
    if footer:
        footer.extract()

    head = soup.find('div', id="mw-head")
    if head:
        head.extract()

    panel = soup.find('div', id="mw-panel")
    if panel:
        panel.extract()

    nav = soup.find('div', id="mw-navigation")
    if nav:
        nav.extract()

    footer = soup.find('div', class_="printfooter")
    if footer:
        footer.extract()

    footer = soup.find('footer')
    if footer:
        footer.extract()

    for script in soup.find_all('span', class_="mw-editsection"):
        script.extract()

    return soup

def make_scheme_lower_and_remove_two_or_more_slashes_and_clean(url):
    parsed = list(urlparse(url))
    parsed[0] = parsed[0].lower()  # make http lower
    parsed[1] = parsed[1].lower()  # make url lower upto .com
    parsed[2] = re.sub("/{2,}", "/", parsed[2])  #remove 2 or more slashes
    cleaned = urlunparse(parsed)
    return cleaned

def remove_port(url):
    result = urlparse(url)
    port = result.port
    if port is not None:
        port = str(result.port)
        new_url = url.strip(port).strip(':')
        return new_url
    else:
        return url


def canonicalization(parent, child):
    url_with_port = make_scheme_lower_and_remove_two_or_more_slashes_and_clean(child)
    url_without_port = remove_port(url_with_port)
    new_url = urljoin(parent, url_without_port)
    url_without_query_string = urlparse(new_url).scheme + "://" + urlparse(new_url).netloc + urlparse(new_url).path
    return url_without_query_string


def crawl_dictionary(id,title,text,inlinks,outlinks):
    inlinks = list(inlinks)
    outlinks = list(outlinks)
    if id in crawldictionary:
        crawldictionary[id]["inlinks"] = set(crawldictionary[id]["inlinks"]) | set(inlinks)
        crawldictionary[id]["inlinks"] = list(crawldictionary[id]["inlinks"])
        crawldictionary[id]["outlinks"] = set(crawldictionary[id]["outlinks"]) | set(inlinks)
        crawldictionary[id]["outlinks"] = list(crawldictionary[id]["outlinks"])
    else:
        crawldictionary[id] = {"title":title,"text":text,"inlinks":inlinks,"outlinks":outlinks}

def crawler():
    seed_url_links = seeds[:]
    crawled_pages = 0
    iteration = 1
    while(crawled_pages <= min_no_of_docs):
        for seed in seeds :
            pprint(seed)

            print("iteration of seed 1:")
            print("no of links from seed 1:" + str(iteration))
            if crawled_pages == min_no_of_docs:
                break
            rp = urllib.robotparser.RobotFileParser()
            domain = urlparse(seed).netloc
            scheme = urlparse(seed).scheme
            rp.set_url(scheme+"://"+domain+"/"+robots_txt)
            try:
                rp.read()
            except IOError:
                pprint ('IOError')
                continue
            except:
                continue
            can_crawl = rp.can_fetch('*', seed)
            #pprint(can_crawl)
            if can_crawl:
                # if on_same_domain:
                time.sleep(1)
                pprint("put to sleep for politeness...")

                try:
                    response = urllib.request.urlopen(seed)
                except IOError:
                    continue
                except UnicodeError:
                    continue
                except:
                    continue
                #pprint(response)
				try:
					lang = response.info().get("Content-language")
					content_type = response.info().get("Content-Type")
					http_headers = str(response.info())
				except:
					continue

                # see if the language of page is english
                #                 pprint(lang, content_type)
                if lang and content_type is not None:
                    if "en" in lang and "text" in content_type:
                        handle = response.read()
						raw_html = str(BeautifulSoup(handle))
                        try:
                            title = soup.title.string
                            title = title.encode('utf-8', 'ignore')
                            title = title.decode('utf-8', 'ignore')
                        except AttributeError:
                            title = ''
                        except:
                            title = ''

                        # get contents of page
                        soup = get_contents_of_page(soup)
                        text = soup.get_text().replace("\n", " ")
                        lines = (line.strip() for line in text.splitlines())
                        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                        text = '\n'.join(chunk for chunk in chunks if chunk)
                        text = text.encode('utf-8', 'ignore')
                        text = text.decode('utf-8')

                        if seed in uncrawled_inlink_map:
                            crawled_urls[seed] = uncrawled_inlink_map[seed]
                        else:
                            crawled_urls[seed] = set()

                        crawled_pages += 1

                        # crawling children
						#storing outlinks
                        out_links = set()
                        for link in soup.find_all('a'):
                            child = link.get('href')
                            if child is None:
                                continue
                            if '#' in child:
                                continue
                            if any(ext in child for ext in not_allowed):
                                continue
                            canonicalised_url = canonicalization(seed, child)
                            #pprint(new_url)
                            canonicalised_url = canonicalised_url.encode('utf-8', 'ignore')
                            canonicalised_url = canonicalised_url.decode('utf-8', 'ignore')
                            out_links.add(canonicalised_url)
			################## end of storing outlinks ####################
			############# adding parent seed as inlink to the outgoing urls from this page ############
                            if canonicalised_url not in crawled_urls:
                                child_links_yet_to_be_crawled.add(canonicalised_url)
                                if canonicalised_url in uncrawled_inlink_map:
                                    uncrawled_inlink_map[canonicalised_url].add(seed)
                                else:
                                    uncrawled_inlink_map[canonicalised_url] = set()
                                    uncrawled_inlink_map[canonicalised_url].add(seed)
                            else:
                                crawled_urls[canonicalised_url].add(seed)
			################################################################

                        ###### link graph for storing inlinks and outlinks##################
			global url_count
			url_count = url_count + 1

			title = filter(lambda x: x in string.printable, title)
			title = ''.join(title)
			text = filter(lambda x: x in string.printable, text)
			text = ''.join(text)
			raw_html = filter(lambda x: x in string.printable, raw_html)
			raw_html = ''.join(raw_html)
			http_header = filter(lambda x: x in string.printable, http_header)
			http_header = ''.join(http_header)


						file_dct[seed] = (title,text,raw_html,http_header)
						#### writing data from 10 urls into a file ....... to avoid memory error 
						if url_count == 10:
							file = "sai"+str(crawled_pages)
							with open(file, "w+") as fc:
								json.dump(file_dct,fc)
							file_dct.clear()
							with open("sai_in_out_links.txt", "w+") as fc:
								json.dump(crawldct,fc)
							f_count = 0

						inlinks = list(uncrawled_inlink_map)
						outlinks = list(out_links)
						
						

						if seed in crawldct:
							crawldct[seed][0] = set(crawldct[seed][0]) | set(inlinks)
							crawldct[seed][0] = list(crawldct[seed][0])
							crawldct[seed][1] = set(crawldct[seed][1]) | set(outlinks)
							crawldct[seed][1] = list(crawldct[seed][1])
						else:
							crawldct[seed] = (inlinks,outlinks)
						###### end of ink graph for storing inlinks and outlinks##################
            iteration += 1
        pprint("next wave ....")
        sorted_child_links = sorted(child_links_yet_to_be_crawled,key=lambda s: len(uncrawled_inlink_map[s]), reverse=True)
        if not sorted_child_links:
            pprint ("Out of out links!, add more seeds")
            break
        del seed_url_links[:]
        seed_url_links = sorted_child_links[:]
        child_links_yet_to_be_crawled.clear()


crawler()
