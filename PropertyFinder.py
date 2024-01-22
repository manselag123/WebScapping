from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
from random import randint
import cv2
from pyzbar.pyzbar import decode
from transformers import pipeline
import re
from selenium.common.exceptions import StaleElementReferenceException
from PIL import Image
from io import BytesIO
import base64 
from datetime import datetime
import pandas as pd
class PropertyFinderScraper:
    def __init__(self):
        self.MAX_RETRIES = 7
        current_date = datetime.now().strftime("%Y-%m-%d") 
        self.excel_data_filename = f"{current_date}_PF_Data.xlsx"
        self.PROPERTIES_PER_PAGE = 25
        self.THRESHOLD_CONFIDENCE = 0.5 
        self.ads =[]
    def extract_page_body_and_filtered_hrefs(self, driver, url, prefix, adtype):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 10)

            # Retry loop to handle StaleElementReferenceException
            for attempt in range(3):  # Adjust the number of attempts as needed
                try:
                    # Locate all elements by tag name 'a'
                    all_links = wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))

                    if all_links:
                        filtered_hrefs = [link.get_attribute('href') for link in all_links if
                                          link is not None and link.get_attribute('href').startswith(prefix)]

                         
                        for href in filtered_hrefs:
                            if all(href != entry['link'] for entry in self.ads):
                                self.ads.append({'link': href, 'type': adtype})
                    return self.ads

                except StaleElementReferenceException:
                    # Handle StaleElementReferenceException by retrying
                    print(f"StaleElementReferenceException. Retrying attempt {attempt + 1}")

            # If retries fail, raise the exception
            raise StaleElementReferenceException("Max retries reached for StaleElementReferenceException")

        except Exception as e:
            print("error")
            raise e
    def extract_page_body_and_filtered_hrefsV1(self, driver, url, prefix, adtype):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            all_links = wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))

            if all_links:
                filtered_hrefs = [link.get_attribute('href') for link in all_links if
                                  link is not None and link.get_attribute('href').startswith(prefix)]

                 
                for href in filtered_hrefs:
                    if all(href != entry['link'] for entry in self.ads):
                        self.ads.append({'link': href, 'type': adtype})
            

        except Exception as e:
            print("error")
            raise e

    def retry_on_failure(self, fn, *args, **kwargs):
        links = []

        for retry in range(self.MAX_RETRIES):
            timeout = 2 ** retry

            try:
                if retry > 1:
                    if driver is not None:
                        driver.quit()
                    chrome_options = webdriver.ChromeOptions()
                    chrome_options.add_argument("--incognito")
                    driver = webdriver.Chrome(options=chrome_options)
                    driver.refresh()
                    driver.quit()
                chrome_options = webdriver.ChromeOptions()
                chrome_options.add_argument("--incognito")
                driver = webdriver.Chrome(options=chrome_options)

                links = fn(driver, *args, **kwargs)
                driver.quit()
                return links

            except Exception as e:
                print(f"Failed on attempt: {retry + 1}. Error: {e}")
                print(*args)
                driver.quit()
                if retry < self.MAX_RETRIES - 1:
                    sleep_time = randint(15, 20)
                    print(f"Retrying in {sleep_time} seconds...")
                    sleep(sleep_time)
                else:
                    print("Failed after max retries!")
                    raise

    def extract_pages_links(self, url, total_pages, all_ads_link, adtype):
        total_pages=1
        for i in range(2, total_pages):
            new_link = url + "&page=" + str(i)
            if new_link not in all_ads_link:
                if all(new_link != entry['link'] for entry in all_ads_link):
                    all_ads_link.append({'link': new_link, 'type': adtype})
        if all(url != entry['link'] for entry in all_ads_link):
            all_ads_link.append({'link': url, 'type': adtype})
        return all_ads_link

    def get_pages_links(self):
        all_pages_links = []
        y_rent_link = "https://www.propertyfinder.ae/en/search?c=2&fu=0&rp=y&ob=mr"
        yrent_pages = self.retry_on_failure(self.extract_page_summary, y_rent_link)
        all_pages_links = self.extract_pages_links(y_rent_link, yrent_pages, all_pages_links, "Rent")

        # m_rent_link = "https://www.propertyfinder.ae/en/search?c=2&fu=0&rp=m&ob=mr"
        # mrent_pages = self.retry_on_failure(self.extract_page_summary, m_rent_link)
        # all_pages_links = self.extract_pages_links(m_rent_link, mrent_pages, all_pages_links, "Rent")

        # buy_link = "https://www.propertyfinder.ae/en/search?c=1&fu=0&ob=mr"
        # buy_pages = self.retry_on_failure(self.extract_page_summary, buy_link)
        # all_pages_links = self.extract_pages_links(buy_link, buy_pages, all_pages_links, "Buy")

        # y_commercial_rent_link = "https://www.propertyfinder.ae/en/search?c=3&fu=0&rp=y&ob=mr"

        # no_pages = self.retry_on_failure(self.extract_page_summary, y_commercial_rent_link)
        # all_pages_links = self.extract_pages_links(y_commercial_rent_link, no_pages, all_pages_links, "Buy")

        # m_commercial_rent_link = "https://www.propertyfinder.ae/en/search?c=3&fu=0&rp=m&ob=mr"
        # no_pages = self.retry_on_failure(self.extract_page_summary, m_commercial_rent_link)
        # all_pages_links = self.extract_pages_links(m_commercial_rent_link, no_pages, all_pages_links, "Rent")

        # commercial_buy_link = "https://www.propertyfinder.ae/en/search?c=3&fu=0&rp=y&ob=mr"
        # no_pages = self.retry_on_failure(self.extract_page_summary, commercial_buy_link)
        # all_pages_links = self.extract_pages_links(commercial_buy_link, no_pages, all_pages_links, "Buy")
        return all_pages_links

    def get_all_ads(self, pages_links):
        prefix_to_filter = "https://www.propertyfinder.ae/en/plp"
        if(pages_links):
            for link in pages_links: 
                self.retry_on_failure(self.extract_page_body_and_filtered_hrefs, link['link'], prefix_to_filter,
                                             link['type'] )
    def extract_page_summary(self,driver,url):  
        driver.get(url)
        xpath = f'//*[@aria-label="Search results count" and contains(text(), "properties")]'  
        desired_element = driver.find_element(By.XPATH,xpath)  
        if(desired_element): 
            total =  desired_element.text.replace(",", "") 
            match = re.search(r'\d+', total) 
            if match: 
                number = int(match.group())  
                total_pages =int( number / self.PROPERTIES_PER_PAGE )  
                return total_pages
        return 0 
    
    
    
    def extract_details(self,driver, link):
        
        try:
            driver.get(link)
            wait = WebDriverWait(driver, 10) 
            # Retry loop to handle StaleElementReferenceException
            for attempt in range(3):  # Adjust the number of attempts as needed
                try: 
                    # Locate all elements by tag name 'a'
                    all_content = "" 
                    text_content = driver.find_element(By.TAG_NAME, 'body').text 
                    all_content = text_content
                    canvas = driver.find_element(By.ID, "react-qrcode-logo") 
                    canvas_content = driver.execute_script("return arguments[0].toDataURL('image/png').substring(21);", canvas) 
                    qrcodeUrl=None
                    image_data = BytesIO(base64.b64decode(canvas_content))
                    image = Image.open(image_data)
                    qr_codes = decode(image)
                    if qr_codes:
                        qr_code_data = qr_codes[0].data.decode("utf-8") 
    
                        # Assuming the URL is in the QR code data
                        url_start_index = qr_code_data.find("https://")
                        if url_start_index != -1:
                            url = qr_code_data[url_start_index:]
                            qrcodeUrl =url 
                    return all_content, qrcodeUrl 
                except StaleElementReferenceException:
                    # Handle StaleElementReferenceException by retrying
                    print(f"StaleElementReferenceException. Retrying attempt {attempt + 1}")

            # If retries fail, raise the exception
            raise StaleElementReferenceException("Max retries reached for StaleElementReferenceException")

        except Exception as e:
            print("error")
            raise e
        
          
        
            
            
            
    def query(self, text, question):
        qa_model = pipeline("question-answering", "timpal0l/mdeberta-v3-base-squad2")
        question = question
        context = text
        result = qa_model(question=question, context=context)

        if result and result['answer']:
            print(result)
            if result['score'] > self.THRESHOLD_CONFIDENCE:
                return result['answer']
            else:
                return None

        return None

    def extract_information_per_content(self, source, content, url, qrcode, adtype):
        BRN = "what is the BRN?"
        Permit_Number = "what is the Permit Number?"
        RERA = "what is the RERA Number?"
        DED = "what is the DED Number?"

        BRN_answer = self.query(content, BRN)
        Permit_Number_answer = self.query(content, Permit_Number)
        RERA_answer = self.query(content, RERA)
        DED_answer = self.query(content, DED)

        data = {
            "Source": source,
            "Content": content,
            "URL": url,
            "QR_URL": qrcode,
            "DED": DED_answer,
            "ListingNumber": Permit_Number_answer,
            "BRN": BRN_answer,
            "RERA": RERA_answer,
            "AD_TYPE": adtype
        }
         
        return data

      
    def extract_PF_ads(self): 
        pages = self.get_pages_links()
        print(f"Pages Length: {0}", len(pages))
        self.get_all_ads(pages)
        print(len(self.ads))
        
        
    def run_extraction(self):
        self.extract_PF_ads() 
        data_frames = [] 
         
        for link_info in self.ads: 
            link = link_info['link']
            ad_type = link_info['type']  
            content, qr_code = self.retry_on_failure(self.extract_details, link)
            data = self.extract_information_per_content("PropertyFinder", content, link, qr_code, ad_type)
            data_frames.append(pd.DataFrame([data]))  
        df = pd.concat(data_frames, ignore_index=True)
        df.to_excel(self.excel_data_filename, index=False) 



 
 
if __name__ == "__main__":
    web_extractor = PropertyFinderScraper()
    web_extractor.run_extraction()
