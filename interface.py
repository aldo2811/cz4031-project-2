from PyQt5 import QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import sys

from annotation import process, init_conn


class ScrollableLabel(QScrollArea):

    def __init__(self, *args, **kwargs):
        QScrollArea.__init__(self, *args, **kwargs)
        widget = QWidget(self)
        self.setWidgetResizable(True)
        self.setWidget(widget)
        layout = QVBoxLayout(widget)
        self.label = QLabel(widget)
        self.label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.label.setFont(QFont('Arial', 15))
        self.label.setStyleSheet("background-color: beige; border: 1px solid black;")
        layout.addWidget(self.label)

    def setText(self, text):
        self.label.setText(text)

    def getValue(self):
        return QScrollArea.verticalScrollBar().value()


class MyWindow(QMainWindow):
    def __init__(self):
        super(MyWindow, self).__init__()
        self.setGeometry(300, 50, 1600, 900)  # xpos, ypos, width, height
        self.setWindowTitle("Application GUI")

        # Output text for query and annotation
        self.queryOutput = ScrollableLabel(self)
        self.queryAnnotate = ScrollableLabel(self)

        # Button for running algorithm
        self.submitButton = QtWidgets.QPushButton(self)

        # Textbox for query and db name
        self.queryTextbox = QTextEdit(self)
        self.dbNameTextbox = QTextEdit(self)

        # Label to indicate which db name is the app currently connected to
        self.dbNameLabel = QtWidgets.QLabel(self)

        # Error message box
        self.error_dialog = QtWidgets.QErrorMessage()

        self.initUI()  # Call initUI

        # Db connection settings
        self.conn = None
        self.dbName = ''

    def initUI(self):
        self.queryOutput.setText("Output Query goes here")
        self.queryOutput.move(10, 400)
        self.queryOutput.resize(770, 450)

        self.queryAnnotate.setText("Annotation goes here")
        self.queryAnnotate.move(820, 400)
        self.queryAnnotate.resize(770, 450)

        self.queryOutput.verticalScrollBar().valueChanged.connect(
            self.queryAnnotate.verticalScrollBar().setValue)
        self.queryAnnotate.verticalScrollBar().valueChanged.connect(
            self.queryOutput.verticalScrollBar().setValue)

        self.queryTextbox.move(10, 20)
        self.queryTextbox.resize(770, 350)
        self.queryTextbox.setFont(QFont('Arial', 15))
        self.queryTextbox.setPlaceholderText('Insert SQL Query Here')

        self.dbNameTextbox.move(820, 140)
        self.dbNameTextbox.resize(200, 100)
        self.dbNameTextbox.setFont(QFont('Arial', 15))
        self.dbNameTextbox.setPlaceholderText('Insert Database Name Here')

        self.dbNameLabel.move(820, 20)
        self.dbNameLabel.resize(200, 100)
        self.dbNameLabel.setFont(QFont('Arial', 15))
        self.dbNameLabel.setText(f"Connected to -")
        self.dbNameLabel.setStyleSheet("background-color: beige; border: 1px solid black;")

        self.submitButton.setText("Submit Query")
        self.submitButton.setFont(QFont('Arial', 15))
        self.submitButton.clicked.connect(self.onClick)
        self.submitButton.move(820, 270)
        self.submitButton.resize(200, 100)

    def onClick(self):
        if self.dbName != self.dbNameTextbox.toPlainText():
            try:
                self.conn = init_conn(self.dbNameTextbox.toPlainText())
                self.dbName = self.dbNameTextbox.toPlainText()
                self.dbNameLabel.setText(f"Connected to {self.dbName}")
            except Exception as e:
                self.error_dialog.showMessage(f"ERROR - {e}")
        if self.conn is not None:
            try:
                query, annotation = process(self.conn, self.queryTextbox.toPlainText())
                self.queryOutput.setText('\n'.join(query))
                self.queryAnnotate.setText('\n'.join(annotation))
            except Exception as e:
                self.error_dialog.showMessage(f"ERROR - {e}")


def window():
    app = QApplication(sys.argv)
    win = MyWindow()

    win.show()
    sys.exit(app.exec_())


def main():
    window()


if __name__ == '__main__':
    main()
