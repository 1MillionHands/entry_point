from table_objects.post import PostHandler
from table_objects.creator import CreatorHandler


class EventHandler:

    def run(self, event):
        return self.route_data_to_object(event)

    @staticmethod
    def route_data_to_object(event):
            # obj = CreatorHandler(event)
            # obj.run('run_from_scooper')

            PostHandler(event).run('run_from_scooper')



if __name__ == "__main__":
    # with open('config_file_.json', 'r') as f:
    #     config_data = json.load(f)
    #     f.close()
    obj = EventHandler()
    obj.run({"id": "2024-09-19 09:19:32.087",'event_name': 'scooper', 'test_env_status': True, 'bucket_name': 'data-omhds', 'input_file': 'test/scooper_imports/2024/09/12/_scooper.json'})

