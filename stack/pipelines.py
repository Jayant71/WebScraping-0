# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from scrapy.utils.project import get_project_settings
import csv
import subprocess


settings = get_project_settings()
from scrapy.exceptions import DropItem

class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]
        

    def process_item(self, item, spider):  # Fixed typo in method name
        valid = True

        for data in item:
            
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.collection.insert_one(dict(item))

        subprocess.run(["mongoexport", "-d", "stackoverflow", "-c", "questions", "--fields", "title,url", "-o", "/home/jayant/Projects/pythoon/stack/output.csv", "--type", "csv", ], text=True)
        
        # self.exportCSV(self.collection)  # if need id in csv file
        return item
    
    def exportCSV(self,collection):        
        cursor = self.collection.find().batch_size(50)

        with open("questions.csv", "w", newline="") as csvfile:  
            csv_writer = csv.writer(csvfile)
            for document in cursor:
                csv_writer.writerow([document["_id"], document["title"], document["url"]])
        for document in cursor:
            flattened_data = [value for field, value in document.items()]  
            csv_writer.writerow(flattened_data)