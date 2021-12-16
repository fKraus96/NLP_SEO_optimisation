
import requests
from datetime import datetime
import pandas as pd


class Requestor():
    user_agent_list = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-en) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"
    ]
    def __init__(self,path):
        self.path = path
        
        self.agent_index = 0
        self.headers = {
            "user-agent": Requestor.user_agent_list[self.agent_index]
        }
        
        #Add as parameter page offset which will change in each iteration to load in new data
        self.params = {
                "page[offset]": 0
        }
        
        #The posts information will be stored here
        self.posts = []
        
        
    def updateList(self, page_n):
        if isinstance(page_n, pd.DataFrame):
            self.posts.append(page_n)
        else: 
            self.posts += page_n
        
    def requestJson(self,path = None):
        if not path:
            path = self.path
        response = requests.get(path, params=self.params, headers=self.headers)
        response.raise_for_status()
                
        return response.json()
        


class Scrape_Discussions(Requestor):
    
    def __init__(self, path, to_date):
        #call init of parent class
        super().__init__(path)
        
        #check that latest date to retrieve is in datetime format
        if not isinstance(to_date,datetime):
            raise Exception("Please specify date in datetime format")
        
        self.to_date = to_date
        
    
    def get_discussion(self):
        #In each iteration add 1 to page offset to load new data until the specified date 
        while True:
            try:
                #Request data from bunq REST API
                data = self.requestJson()["data"]
                
                #Update the output list
                self.updateList(data)
                print(f"Page {self.params['page[offset]']} loaded")
                self.params["page[offset]"] += len(data)
                
                
            except:
                #If request fails change user agent and try again
                self.agent_index +=1 
                self.headers["user_agent"] = Requestor.user_agent_list[self.agent_index]
                continue
        
            last_date = datetime.strptime(data[-1]["attributes"]["lastPostedAt"] ,"%Y-%m-%dT%H:%M:%S+00:00")
            
            #End the loop when the data generated is further back than the specified date
            if last_date < self.to_date:
                self.page_len = len(data)
                break
        
    
    def generate(self):
        #Remove any older posts from the last iteration - Done it this way for performance reason rather than checking date of each post each time in get_discussion
        self.posts = self.posts[:-self.page_len] + [post for post in self.posts[-self.page_len:] if  datetime.strptime(post["attributes"]["lastPostedAt"],"%Y-%m-%dT%H:%M:%S+00:00") < self.to_date]
        yield from self.posts
    
    

    


# In[ ]:


class Scrape_Posts(Requestor):
    out_order = ["title", "posted_at","created_At","contentType","user_id", "username", "tags", "votes", "is_pinned","is_locked","contentHtmlTranslated"]
    def __init__(self,path, discussion_gen):
        #call init of parent class 
        super().__init__(path)
        self.discussion_gen = discussion_gen
        
    def get_posts(self):
        #Loop through all discussions retrieved
        for i,discussion in enumerate(self.discussion_gen):
            if i%100 == 0:
                print(f"Retriving data of post {i}")
            try :
                #make request to particular get particular discussion
                url = f"{self.path}/{discussion['id']}"
                post = self.requestJson(url)

                #Get all relevant data from post type
                post_info = self.__post_process(post)

                #Ger all relevant data from user type
                user_info = self.__user_process(post)

                #Get tags names
                tags_info = self.__tag_process(post)

                out_df = post_info.merge(user_info,left_on= "user_id", right_on="id", how = "left")
                out_df["tags"] = [tags_info] * len(out_df)

                meta_data = post["data"]["attributes"]
                self.__add_metadata(out_df,meta_data)
                
                self.updateList(out_df)
            except:
                self.agent_index +=1 
                self.headers["user_agent"] = Requestor.user_agent_list[self.agent_index]
                print("Switched user agent")
                continue
    
    def posts_df(self):
        return pd.concat(self.posts,axis = 0).reset_index(drop = True)[Scrape_Posts.out_order]
            
    def __add_metadata(self,out_df,meta_data):
        out_df["title"] = meta_data["titleTranslated"]
        out_df["votes"] = meta_data["votes"]
        out_df["is_pinned"] = meta_data["isSticky"]
        out_df["is_locked"] = meta_data["isLocked"]

            
    def __post_process(self,post):
        #Create Dataframe and get all the ones of type post
        posts = pd.DataFrame(post["included"])
        posts = posts.loc[posts["type"] == "posts"]
        
        #Open up attributes dictionary to columns
        post_info = pd.concat([posts[["id"]],posts["attributes"].apply(pd.Series)],axis = 1)
        
        #Posted at in datetime format
        post_info["posted_at"] = post_info["createdAt"].apply(pd.to_datetime)

        #Take only ones with actual text
        post_info = post_info.loc[post_info["contentType"] == "comment",["contentType","contentHtmlTranslated","posted_at"]]
        
        #First post is the post that starts discussion - should be marked as such for further analyiss
        post_info.loc[0,"contentType"] = "start_thread"
        post_info["created_At"] = post_info.loc[0,"posted_at"]
        
        #Retrieve user ID of post from relationshsips for later merging
        post_info["user_id"] = posts["relationships"].apply(pd.Series)["user"].apply(lambda x: x["data"]["id"])
        
        return post_info
    
    
    def __user_process(self,post):
        #Create Dataframe and get all the ones of type user
        user_info = pd.DataFrame(post["included"])
        user_info = user_info.loc[user_info["type"] == "users"]
        
        #Open up attributes dictionary to columns and take relevant columns
        user_info = pd.concat([user_info[["id"]],user_info["attributes"].apply(pd.Series)],axis = 1)
        user_info = user_info[["id","username"]]
        
        return user_info
    
    def __tag_process(self,post):
        tags = pd.DataFrame(post["included"])
        tags = tags.loc[tags["type"] == "tags"]
        return list(tags["attributes"].apply(lambda x: x["name"]))
            

