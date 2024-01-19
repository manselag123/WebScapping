from selenium import webdriver
from selenium.webdriver.common.by import By
from pyquery import PyQuery as pq
import re
import pandas as pd
from datetime import datetime
import cv2
from pyzbar.pyzbar import decode
import random
from transformers import pipeline
from difflib import SequenceMatcher

# Constants
BASE_URL = "https://www.bayut.com/"
TARGET_ARIA_LABEL = "Listing link"
SUMMARY_LABEL = "Summary text"
PROPERTIES_PER_PAGE = 24
THRESHOLD_CONFIDENCE = 0.5
current_date = datetime.now().strftime("%Y-%m-%d") 
# Save the DataFrame to an Excel file with the current date as the filename
 
excel_data_filename = f"{current_date}_Bayut_Data.xlsx"
class WebExtractor:
    def __init__(self):
        self.href_list = []

    def extract_links_per_page(self, pagelink, is_first, total_pages, adtype):
        try:
            
            page_ = pq(url=pagelink)  # Specify the aria-label value you want to select 
        # Find all elements with the specified aria-label value
            elements_with_specific_aria_label = page_('[aria-label="{0}"]'.format(TARGET_ARIA_LABEL))
            if(is_first): 
                elements_summaries = page_(f'[aria-label="{SUMMARY_LABEL}"]')  
                total_properties =  elements_summaries.text().split('of')[-1].strip()   
                total =  total_properties.replace(",", "")  
                match = re.search(r'\d+', total) 
                if match: 
                    number = int(match.group()) 
                    total_pages =int( number / PROPERTIES_PER_PAGE)   
            if elements_with_specific_aria_label: 
            # Iterate through matched elements and get the href value for each
                for element in elements_with_specific_aria_label:
                    href_value = pq(element).attr('href')
                    if href_value not in self.href_list:  
                        if 'https://www.bayut.com' in href_value:
                            href=href_value 
                        else:
                            href="https://www.bayut.com"+href_value  
                        if all(href != entry['link'] for entry in self.href_list): 
                            self.href_list.append({'link': href, 'type': adtype})  
                return total_pages
            else:
                print('No elements with aria-label "{0}" found.'.format(TARGET_ARIA_LABEL)) 
        except Exception as e:
            print('An error occurred:', str(e))

    def extract_all_links(self, ad_type, frequent):
        total_pages = 0
        total_pages = self.extract_links_per_page(f"{BASE_URL}{ad_type}/property/uae/?rent_frequency={frequent}",
                                                  True, total_pages, ad_type)
        print(total_pages)
        # if total_pages > 0:
        #     for i in range(1, total_pages):
        #         print(i)
        #         page_link = f"{BASE_URL}{ad_type}/property/uae/page-{i + 1}/?rent_frequency={frequent}"
        #         print(page_link)
        #         total_pages = self.extract_links_per_page(page_link, False, total_pages, ad_type)
        #     else:
        #         print("no pages found")

        return self.href_list

    def extract_details(self, link):
        driver = webdriver.Chrome()
        try:
            all_content = ""
            driver.get(link)
            text_content = driver.find_element(By.TAG_NAME, 'body').text
            all_content = text_content

            found_svg_element = None
            svg_elements = driver.find_elements(By.TAG_NAME, 'svg')
            if svg_elements:
                for i, svg_element in enumerate(svg_elements):
                    svg_content = svg_element.get_attribute('outerHTML')
                    if 'image' in svg_content:
                        found_svg_element = svg_element
                        svg_location = svg_element.location
            if found_svg_element:
                driver.execute_script("window.scrollTo(0, arguments[0].getBoundingClientRect().top - 160);",
                                      found_svg_element)
                driver.get_screenshot_as_file("screenshot.png")
                svg_image = cv2.imread("screenshot.png")
                gray = cv2.cvtColor(svg_image, cv2.COLOR_BGR2GRAY)

                qr_codes = decode(gray)

                if qr_codes:
                    qr_code_data = qr_codes[0].data.decode("utf-8")
                    return all_content, qr_code_data
                else:
                    return all_content, None
        finally:
            driver.quit()

    def query(self, text, question):
        qa_model = pipeline("question-answering", "timpal0l/mdeberta-v3-base-squad2")
        question = question
        context = text
        result = qa_model(question=question, context=context)

        if result and result['answer']:
            print(result)
            if result['score'] > THRESHOLD_CONFIDENCE:
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
        print(data)
        return data

    def run_extraction(self, ad_type, frequent):
        all_links = self.extract_all_links(ad_type, frequent)
        df_links = pd.DataFrame(all_links, columns=["URL", "AD_TYPE"])

        shuffled_list = random.sample(all_links, len(all_links))
        data_frames = []
        two_links = shuffled_list[:2]
        for link in two_links:
            content, qr_code = self.extract_details(link["link"])
            data = self.extract_information_per_content("Bayut", content, link["link"], qr_code, link['type'])
            data_frames.append(pd.DataFrame([data]))
        
        df = pd.concat(data_frames, ignore_index=True)
        df.to_excel(excel_data_filename, index=False)
if __name__ == "__main__":
    web_extractor = WebExtractor()
    web_extractor.run_extraction("to-rent", "any")
    web_extractor.run_extraction("for-sale", "any")
