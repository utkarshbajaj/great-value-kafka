import requests
import json
import threading


def create_topic(topic_name):
    topic_url = f"http://localhost:9696/topics"
    body = {"name": topic_name}
    response = requests.post(topic_url, json=body)
    return response


def create_consumer_group(topic_name, cg_id):
    # create a new consumer group
    consumer_url = f"http://localhost:9696/topics/{topic_name}/subscribe"
    response = requests.post(consumer_url)

    # get the consumer group id which is a byte of uuid
    cg_id = response.content.decode("utf-8")
    return cg_id


def publish_messages(topic_name, message, num_messages):
    responses = []
    for i in range(num_messages):
        m = f"{message}{i}"
        publish_url = f"http://localhost:9696/topics/{topic_name}/publish"
        payload = {"message": m}
        response = requests.post(publish_url, json=payload)
        responses.append(response)
    return responses


def create_subscriber(topic_name, cg_id):
    # create a new subscriber
    subscriber_url = (
        f"http://localhost:9696/topics/{topic_name}/consumer-groups/{cg_id}/subscribe"
    )
    response = requests.post(subscriber_url)
    sub_id = response.content.decode("utf-8")
    return sub_id


# this function should be a thread function
def read_messages(topic_name, cg_id, sub_id):
    consumer_url = f"http://localhost:9696/topics/{topic_name}/consumer-groups/{cg_id}/subscribers/{sub_id}"
    response = requests.get(consumer_url)
    data = json.loads(response.content)
    print(data)
    return data


if __name__ == "__main__":
    topic_name = "cats"
    ok = create_topic(topic_name)
    cg_id = create_consumer_group(topic_name, "cats")
    sub_ids = []
    for i in range(5):
        sub_id = create_subscriber(topic_name, cg_id)
        sub_ids.append(sub_id)
    responses = publish_messages(topic_name, "meow", 10)

    while True:
        # ask if they want to read (r) or publish (p)
        print("Press r to read, p to publish")
        choice = input()

        match choice:
            case "r":
                for sub_id in sub_ids:
                  thread = threading.Thread(target=read_messages, args=(topic_name, cg_id, sub_id))
                  thread.start()
            case "p":
                num_messages = int(input("How many meows do you want to publish? "))
                publish_messages(topic_name, "meow", num_messages)
            case _:
                print("Invalid choice")
