import scrapy
from scrapy.crawler import CrawlerProcess
from time import sleep

class Spinarak(scrapy.Spider):
    name = "Spinarak"
    custom_settings = {'CONCURRENT_REQUESTS_PER_DOMAIN' : 1, 'DUPEFILTER_CLASS' : 'scrapy.dupefilters.BaseDupeFilter'}
    link = 'https://bulbapedia.bulbagarden.net/wiki/Bulbasaur_(Pok%C3%A9mon)'
    visited = set()
    main_data = []
    types_list = [] # list of tuples
    stats_list = [] # list of dicts
    abilities_list = [] #list of abilities
    moves_list = []#list of all moves
    first_pass = True
    next_mon_link = ''
    def start_requests(self):     
        yield scrapy.Request(url=self.link, callback = self.parse)


    async def parse(self, response):
        page_content = response.css('div#mw-content-text div.mw-content-ltr')
        if self.first_pass:
            self.first_pass = False
            yield scrapy.Request(url='https://bulbapedia.bulbagarden.net/wiki/List_of_Pok%C3%A9mon_by_National_Pok%C3%A9dex_number', callback = self.get_types)
            yield scrapy.Request(url='https://bulbapedia.bulbagarden.net/wiki/List_of_Pok%C3%A9mon_by_base_stats_in_Generation_IX', callback = self.get_stats)
            yield scrapy.Request(url='https://bulbapedia.bulbagarden.net/wiki/List_of_Pok%C3%A9mon_by_Ability', callback = self.get_abilities)
            yield scrapy.Request(url='https://bulbapedia.bulbagarden.net/wiki/List_of_moves', callback = self.get_all_moves)
        
        if page_content.xpath('./table[1]/tbody/tr[2]/td[3]/table/tbody/tr/td/a/@href').get() or len(self.main_data) == 1024:
            self.next_mon_link = page_content.xpath('./table[1]/tbody/tr[2]/td[3]/table/tbody/tr/td/a/@href').get()
            print(self.next_mon_link)
            self.parse_main(page_content)
            print(f'parsed: {len(self.main_data)}/1025')
            if self.next_mon_link:
                yield response.follow(url = self.next_mon_link, callback = self.parse)
            
            elif len(self.main_data) == 1025:
                #add types to main_data
                print('adding type data')
                for i, v in enumerate(self.types_list):
                    self.main_data[i]['type_1'] = v[0]
                    if v[1] != None:
                        self.main_data[i]['type_2'] = v[1]
                    else:
                        self.main_data[i]['type_2'] = 'None'
                print('adding stats data')
                #add stats to main_data
                for i, row in enumerate(self.stats_list):
                    for k, v in row.items():
                        self.main_data[i][k] = v
                print('adding ability data')
                #add abilities to main_data
                for i, row in enumerate(self.abilities_list):
                    for j in range(3):
                        if row[j] != None:
                            self.main_data[i][f'ability_{j+1}'] = row[j]
                        else:
                            self.main_data[i][f'ability_{j+1}'] = 'None'
                print('writing pokedex.csv')
                with open('Pokedex.csv', 'w', encoding='utf-8') as file:
                    for k in self.main_data[0].keys():
                        file.write(k + ', ')
                    file.write('\n')
                    for mon in self.main_data:
                        for k,v in mon.items():
                            file.write(v + ', ')
                        file.write("\n")
                    file.close()
                print('writing moves.csv')
                with open('Moves.csv', 'w', encoding='utf-8') as file:
                    for k in self.moves_list[0].keys():
                        file.write(k + ', ')
                    for move in self.moves_list:
                        for k,v in move.items():
                            file.write(v + ', ')
                        file.write("\n")
                    file.close()
                return
        else:
            sleep(10)
            yield response.follow(url = self.next_mon_link, callback = self.parse)


    def parse_main(self, page_content):
        data = {}
        name_num_img = page_content.xpath('./table[2]/tbody/tr[1]/td/table/tbody')
        data['name_en'] = name_num_img.xpath('./tr[1]/td[1]/table/tbody/tr/td[1]/big/big/b/text()').get()
        data['category'] = name_num_img.xpath('./tr[1]/td[1]/table/tbody/tr/td[1]/a/span/text()').get()
        data['name_jp'] = name_num_img.xpath('./tr[1]/td[1]/table/tbody/tr/td[2]/span/b/text()').get()
        if not data['name_jp']:
            data['name_jp'] = name_num_img.xpath('./tr[1]/td[1]/table/tbody/tr/td[2]/span/b/span/text()').get()
        data['name_jp_phonetic'] = name_num_img.xpath('./tr[1]/td/table/tbody/tr/td[2]/i/text()').get()
        data['dex_num'] = name_num_img.xpath('./tr[1]/th//span/text()').get()
        data['photo_link'] = name_num_img.xpath('./tr[2]/td/table/tbody/tr[4]//a/@href').get()#can be used to scrape images later
        data['type_1'] = ''
        data['type_2'] = ''
        data['ability_1'] = ''
        data['ability_2'] = ''
        data['ability_3'] = ''
        data['hp'] = ''
        data['attack'] = ''
        data['defense'] = ''
        data['special attack'] = ''
        data['special defense'] = ''
        data['speed'] = ''
        data['learnset'] = ''
        #get move data delimited by h3 elements between Learnset and Side game data
        learnset = []
        learnset.append(page_content.xpath('//span[@id="Learnset"]/../following-sibling::*[1]'))
        while learnset[-1].xpath('name()').get() != 'h3':
            if learnset[-1].xpath('./following-sibling::*[1]//text()').get() == 'TCG':
                break
            learnset.append(learnset[-1].xpath('./following-sibling::*[1]'))
        for i, elem in enumerate(learnset):
            if elem.xpath('name()').get() == 'table':
                if learnset[i-1].xpath('name()').get() == 'h5':
                    if learnset[i-1].xpath('.//text()').get() == data['name_en']:
                        heading = 'learnset'
                    else:
                        continue
                else:
                    heading = 'learnset'
                data[heading] += self.get_moves_from_table(elem, data)
        data['learnset'] = data['learnset'][:-2]
        self.main_data.append(data)
        return


    def get_moves_from_table(self, table, data):
        moves_col = 0
        for i, col in enumerate(table.xpath('./tbody/tr[2]/td/table/tbody/tr/th'), 1):
            if col.xpath('./a/span/text()').get() == "Move":
                moves_col = i
                break
        moves = ''
        for row in table.xpath('./tbody/tr[2]/td//table/tbody/tr')[1:]:
            if row.xpath(f'./td[{moves_col}]').get():
                moves += row.xpath(f'./td[{moves_col}]//text()').get() + ', '
        return moves


    def get_types(self, response):
        tables = response.xpath('.//div[@id="mw-content-text"]/div[1]/table[1]/following-sibling::table')
        for table in tables[:-1]:
            table_rows = iter(table.xpath('./tbody/tr'))
            next(table_rows) #skip column header
            for row in table_rows:
                self.types_list.append([row.xpath('./td[4]/a/span/text()').get(), row.xpath('./td[5]/a/span/text()').get()])
                if not row.xpath('./td[1]/@rowspan').get() == '1':
                    for i in range(int(row.xpath('./td[1]/@rowspan').get())-1): #call next() on iterator to skip extra forms
                        next(table_rows)
        

    def get_stats(self, response):
        table_rows = response.xpath('.//div[@id="mw-content-text"]/div[1]/table[3]/tbody/tr')
        for row in table_rows[1:]:
            stats = {}
            if row.xpath('./td[1]/text()').get() == row.xpath('./preceding-sibling::tr[1]/td[1]/text()').get(): #skip megas and regional forms
                continue
            stats['hp'] = row.xpath('./td[4]/text()').get().removesuffix('\n')
            stats['attack'] = row.xpath('./td[5]/text()').get().removesuffix('\n')
            stats['defense'] = row.xpath('./td[6]/text()').get().removesuffix('\n')
            stats['special attack'] = row.xpath('./td[7]/text()').get().removesuffix('\n')
            stats['special defense'] = row.xpath('./td[8]/text()').get().removesuffix('\n')
            stats['speed'] = row.xpath('./td[9]/text()').get().removesuffix('\n')
            self.stats_list.append(stats)


    def get_abilities(self, response):
        tables = response.xpath('.//div[@id="mw-content-text"]/div[1]/table')
        for table in tables[:-1]:
            rows = table.xpath('./tbody/tr/td/table/tbody/tr')
            for row in rows:
                if row.xpath('./td[1]/text()').get() == row.xpath('./preceding-sibling::tr[1]/td[1]/text()').get():
                    continue
                abilities = \
                [
                    row.xpath('./td[4]/a/text()').get(),
                    row.xpath('./td[5]/a/text()').get(),
                    row.xpath('./td[6]/a/text()').get()
                ]
                self.abilities_list.append(abilities)


    def get_all_moves(self, response):
        rows = response.xpath('.//div[@id="mw-content-text"]/div[1]/table[1]/tbody/tr/td/table/tbody/tr')
        for row in rows[1:]:
            move = {}
            move['name'] = row.xpath('./td[2]/a/text()').get()
            move['type'] = row.xpath('./td[3]/a/span/text()').get()
            move['category'] = row.xpath('./td[4]/a/span/text()').get()
            move['pp'] = row.xpath('./td[5]/text()').get().removesuffix('\n')
            move['power'] = row.xpath('./td[6]/text()').get().removesuffix('\n')
            move['accuracy'] = row.xpath('./td[7]/text()').get().removesuffix('\n')
            self.moves_list.append(move)


spinarak = CrawlerProcess()
spinarak.crawl(Spinarak)
spinarak.start()