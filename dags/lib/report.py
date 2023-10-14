import pdfkit

class Report:
    def __init__(self, context):
        self.context = context
    
    def content(self):
        # Write content of report in html format
        return str("""
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Task Details</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        background-color: #96DCFF;
                        margin: 20px;
                    }}

                    .task-container {{
                        margin: 2%;
                        padding: 20px;
                        background-color: #fff;
                        border-radius: 8px;
                        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                    }}

                    h2 {{
                        color: #333;
                    }}

                    p {{
                        margin: 0;
                        color: #555;
                    }}

                    .exception {{
                        color: red;
                    }}

                    img {{
                        width: 175px;
                    }}
                </style>
            </head>
            <body>
                <div class="task-container">
                    <h2>Report of Task</h2>
                    <p>Dear all, below we send the latest information on the ongoing DAG Airflow process:</p>
                    <br>
                    <p><strong>DAG:</strong> {0}</p>
                    <p><strong>Task:</strong> {1}</p>
                    <p><strong>Executed Date:</strong> {2}</p>
                    <p><strong>Duration:</strong> {3}</p>
                    <p><strong>Details:</strong> <span class="exception">{4}</span></p>
                    <br>
                    <P>The following is an attachment of information about the running process, this file maybe strictly confidential.
                        Please do not distribute it widely outside the company.</P>
                    <br>
                    <P>Best Regards,</P>
                    <img src="https://drive.google.com/uc?id=192nL0cpT2uM5aKnrxiR8tFGzjNewAR54">
                </div>
            </body>
            </html>
        """.format(
            self.context['task_instance_key_str'].split('__')[0],
            self.context['task_instance_key_str'].split('__')[1],
            self.context['task_instance_key_str'].split('__')[2],
            self.context['task_instance'].duration,
            self.context['exception'] if 'exception' in self.context else 'Almost Done!'
            )
        )

    def generate_pdf(self):
        # Generate PDF report to send to client
        pdfkit.from_string(self.content(), 'report.pdf')