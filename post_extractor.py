from abc import ABC, abstractmethod
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from post_model import Post


class PostExtractor(ABC):
    # post_element: WebElement
    driver: WebDriver

    def __init__(self,  driver: WebDriver):
        # self.post_element = post_element
        self.driver = driver       

    
    @abstractmethod
    def extract_post_time_crawl(self):
        pass
    
    @abstractmethod
    def extract_post_author(self):
        pass

    @abstractmethod
    def extract_post_author_link(self):
        pass

    @abstractmethod
    def extract_post_author_avatar(self):
        pass

    @abstractmethod
    def extract_post_created_time(self):
        pass

    @abstractmethod
    def extract_post_content(self):
        pass

    @abstractmethod
    def extract_post_link(self):
        pass

    @abstractmethod
    def extract_post_id(self):
        pass
    
    @abstractmethod
    def extract_post_like(self):
        pass
    
    @abstractmethod
    def extract_post_love(self):
        pass
    
    @abstractmethod
    def extract_post_comment(self):
        pass
    
    @abstractmethod
    def extract_post_share(self):
        pass
    
    @abstractmethod
    def extract_post_domain(self):
        pass

    @abstractmethod
    def extract_post_hashtag(self):
        pass
    
    @abstractmethod
    def extract_post_music(self):
        pass
    
    @abstractmethod
    def extract_post_duration(self):
        pass
    
    @abstractmethod
    def extract_post_view(self):
        pass
    
    @abstractmethod
    def extract_post_type(self):
        pass
    
    @abstractmethod
    def extract_post_source_id(self):
        pass
    
    @abstractmethod
    def extract_post_title(self):
        pass

    def extract(self) -> Post:
        post = Post()
        time_crawl = self.extract_post_time_crawl()
        link = self.extract_post_link()
        id = self.extract_post_id()
        author = self.extract_post_author()
        author_link = self.extract_post_author_link()
        author_avatar = self.extract_post_author_avatar()
        created_time = self.extract_post_created_time()
        content = self.extract_post_content()
        title = self.extract_post_title()
        like = self.extract_post_like()
        love = self.extract_post_love()
        comment = self.extract_post_comment()
        share = self.extract_post_share()
        domain = self.extract_post_domain()
        hashtag = self.extract_post_hashtag()
        music = self.extract_post_music()
        duration = self.extract_post_duration()
        view = self.extract_post_view()
        type = self.extract_post_type()
        source_id = self.extract_post_source_id()

        
        # comments = self.extract_post_comments()
        # image_links = self.extract_post_photos()
        post.id = id
        post.link = link
        post.time_crawl = time_crawl
        post.author = author
        post.author_link = author_link
        post.avatar = author_avatar
        post.created_time = created_time
        post.content = content
        post.title = title
        post.like = like
        post.comment = comment
        post.love = love
        post.share = share
        post.domain = domain
        post.hashtag = hashtag
        post. music = music
        post.duration = duration
        post.view = view
        post.type = type
        post.source_id = source_id

        # post.image_url = image_links
        # post.dataInListComments = comments

        return post     
    
