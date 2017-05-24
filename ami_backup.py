from __future__ import print_function

import boto3
import distutils.util
from datetime import datetime

DEFAULT_BACKUP_GENERATION = 7


def ami_back_up(ec2, client, instance_id):
    instance = ec2.Instance(instance_id)
    instance_name, image_name = make_name(instance)
    print('create image {}'.format(image_name))
    image = instance.create_image(
        Name=image_name,
        NoReboot=no_reboot(instance)
    )
    set_tags_to_image(ec2, image, instance_name)

    sorted_images = sort_images_by_createtime(client, instance_name)
    delete_images = sorted_images[get_backup_generation(instance):]
    if len(delete_images) > 0:
        delete_old_images(client, delete_images)


def listup_backup_instances(client):
    response = client.describe_instances(
        Filters=[
            {
                'Name': 'tag-key',
                'Values': ['BACKUP_GENERATION']
            }
        ]
    )
    instance_ids = []
    instances = response['Reservations']
    for instance in instances:
        instance_ids.append(instance['Instances'][0]['InstanceId'])
    return instance_ids


def make_name(instance):
    for tag in instance.tags:
        if tag['Key'] == 'Name':
            return tag['Value'], tag['Value'] + '-' + get_time_now()


def no_reboot(instance):
    for tag in instance.tags:
        if tag['Key'] == 'BACKUP_NO_REBOOT':
            if tag['Value'] in ('True', 'False'):
                return True if distutils.util.strtobool(tag['Value']) else False
            else:
                print('[WARN]: BACKUP_NO_REBOOT tag is invalid')
                return True
    return True


def get_backup_generation(instance):
    for tag in instance.tags:
        if tag['Key'] == 'BACKUP_GENERATION':
            return int(tag['Value'])
    return DEFAULT_BACKUP_GENERATION


def get_time_now():
    return datetime.now().strftime("%Y%m%d%H%M")


def delete_old_images(client, images):
    for image in images:
        print('delete image {}'.format(image['Name']))
        response = client.deregister_image(
            DryRun=False,
            ImageId=image['ImageId']
        )


def set_tags_to_image(ec2, image, name):
    image.create_tags(
        Tags=[
            {
                'Key': 'Name',
                'Value': name
            },
            {
                'Key': 'CreateTime',
                'Value': get_time_now()
            }
        ]
    )


def sort_images_by_createtime(client, name):
    response = client.describe_images(
        Filters=[
            {
                'Name': 'tag:Name',
                'Values': [name]}
        ]
    )

    images = response['Images']
    sorted_images = sorted(
        images,
        key=lambda x: x['CreationDate'],
        reverse=True
    )
    return sorted_images


def lambda_handler(event, context):
    client = boto3.client('ec2')
    ec2 = boto3.resource('ec2')
    instance_ids = listup_backup_instances(client)
    for i in instance_ids:
        ami_back_up(ec2, client, i)
