import json
from time import sleep
from bs4 import BeautifulSoup as bs
import requests

current_id = 0

def load_tags():
    tags_dict = {}
    try:
        with open("tags_full.txt", "r", encoding="utf-8") as f:
            for line in f:
                if ':' in line:
                    id_str, tags_line = line.strip().split(':', 1)
                    tags_dict[int(id_str)] = tags_line.strip()
    except Exception as e:
        print("Ошибка при загрузке тегов:", e)
    return tags_dict

def parse_data(u):
    global current_id 
    
    name = '-'
    description = '-'
    image = '-'
    tags = '-'

    try:
        page = requests.get(u)
        soup = bs(page.text, "html.parser")

        headline = soup.find(attrs={"itemprop": "headline"})
        if headline:
            name = headline.get_text(strip=True)

        article = soup.find(attrs={"itemprop": "articleBody"})
        if article:
            first_p = article.find("p")
            if first_p:
                description = first_p.get_text(strip=True)

        figure = soup.find("figure", class_="s-post-media-img post-thumbnail post-media-b")
        if figure:
            img = figure.find("img")
            if img and img.get("src"):
                image = img["src"]

    except Exception as e:
        print("Ошибка при парсинге:", e)

    current_id += 1
    if current_id in tags_dict:
        tags = tags_dict[current_id]

    index_meta = json.dumps({
        "index": {
            "_index": "first_index",
            "_id": current_id
        }
    }, ensure_ascii=False)

    record = json.dumps({
        "id": str(current_id),
        "name": name,
        "images": image,
        "description": description,
        "tags": tags
    }, ensure_ascii=False)

    return index_meta, record

def parse_href(url,tags_dict):
    response = requests.get(url)
    html = response.text
    soup = bs(html, 'html.parser')
    container = soup.find('div', class_="bb-col col-content")
    posts = container.find_all("article", class_="post")
    with open("json.json", "a", encoding="utf-8") as f:
        for post in posts:
            a_tag = post.find("a", href=True)
            if a_tag:
                index_meta, record = parse_data(a_tag['href'])
                f.write(f"{index_meta}\n{record}\n")
        f.write("\n")
        
def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)

if __name__ == '__main__':
    base_url = "https://memepedia.ru/category/memes/pic/page/"
    
    tags_dict = load_tags()

    for i in range(1, 11):
        url = f"{base_url}{i}"
        parse_href(url, tags_dict)
        sleep(1)
