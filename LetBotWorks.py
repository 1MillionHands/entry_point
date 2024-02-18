from PostAPI import PostAPI


class LetBotWork(PostAPI):

    def get_headers(self):
        return {"accept": "application/json",
                "Cookie": f"CF_Authorization={self.config_data['letBotsWorkToekn']}"}

    def get_post_information(self, url):
        headers = self.get_headers()
        return self.session.get(url, headers=headers, timeout=50)

    def get_url(self):
        page_size = self.config_data.get("page_size")
        return f'https://api.oct7.io/posts?sort=created_at.desc&limit={page_size}'

