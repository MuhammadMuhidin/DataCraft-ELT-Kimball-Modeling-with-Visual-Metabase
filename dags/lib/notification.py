from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email import EmailOperator
from lib.report import Report

class Notification:
    def __init__(self, context):
        self.context = context
        self.subject = self.get_subject()
        self.content = self.get_content()

    def get_subject(self):
        # Get the status of the task instance to be a subject email with according conditions
        # If the state is success then return success, if the state is up_for_retry then return retry, else return failure
        if self.context['task_instance'].state == 'success':
            return "[Airflow Success] ✅ [{0} {1}] duration {2}".format(
                self.context['task_instance_key_str'].split('__')[0],
                self.context['task_instance_key_str'].split('__')[1],
                self.context['task_instance'].duration
            )
        elif self.context['task_instance'].state == 'up_for_retry':
            return "[Airflow Retry] ⚠️ [{0} {1}] duration {2}".format(
                self.context['task_instance_key_str'].split('__')[0],
                self.context['task_instance_key_str'].split('__')[1],
                self.context['task_instance'].duration
            )
        else:
            return "[Airflow Failure] ⛔ [{0} {1}] duration {2}" .format(
                self.context['task_instance_key_str'].split('__')[0],
                self.context['task_instance_key_str'].split('__')[1],
                self.context['task_instance'].duration
            )

    def get_content(self):
        # Get value of task instance to be add as the content email
        return "DAG : {0} <br>Task : {1} <br>Executed Date : {2}<br>Duration : {3}<br>Details : {4}".format(
            self.context['task_instance_key_str'].split('__')[0],
            self.context['task_instance_key_str'].split('__')[1],
            self.context['task_instance_key_str'].split('__')[2],
            self.context['task_instance'].duration,
            self.context['exception'] if 'exception' in self.context else 'Almost Done 👍'
        )

    def send_email(self):
        # Send email with the subject and content and files report
        email = EmailOperator(
            task_id = 'task_send_email',
            to = 'muhammadmuhidin222@gmail.com',
            subject = self.subject,
            html_content = self.content,
            files = ['report.pdf']
        )
        return email.execute(context=self.context)

    def send_telegram(self):
        # Send telegram with the subject of the email
        telegram = TelegramHook(telegram_conn_id='telegram_default')
        return telegram.send_message({'text': self.subject, 'parse_mode': 'HTML'})

    def send_slack(self):
        # Send slack with the subject of the email
        slack = SlackWebhookOperator(
            task_id='task_send_slack',
            message=self.subject,
            slack_webhook_conn_id='slack_default'
        )
        return slack.execute(context=self.context)

    @staticmethod
    def push(context):
        report = Report(context)
        notification = Notification(context)
        report.generate_pdf()
        notification.send_email()
        notification.send_telegram()
        notification.send_slack()




