from PyQt5 import QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
# from PyQt5.QtWidgets import QApplication, QMainWindow, QTextEdit
import sys


class MyWindow(QMainWindow):
    def __init__(self):
        super(MyWindow, self).__init__()
        self.setGeometry(300, 50, 900, 900)  # xpos, ypos, width, height
        self.setWindowTitle("Application GUI")
        self.initUI()  # Call initUI

    def initUI(self):
        # Output text for query and annotation
        self.queryOutput = QtWidgets.QLabel("Output Query goes here", self)
        self.queryOutput.move(10, 450)
        self.queryOutput.resize(400, 400)
        self.queryOutput.setFont(QFont('Arial', 15))
        self.queryOutput.setStyleSheet("background-color: beige; border: 1px solid black;")

        self.queryAnnotate = QtWidgets.QLabel("Annotation goes here", self)
        self.queryAnnotate.move(420, 450)
        self.queryAnnotate.resize(400, 400)
        self.queryAnnotate.setFont(QFont('Arial', 15))
        self.queryAnnotate.setStyleSheet("background-color: beige; border: 1px solid black;")

        # Textbox for query
        self.queryTextbox = QTextEdit(self)
        self.queryTextbox.move(10, 20)
        self.queryTextbox.resize(400, 400)
        self.queryTextbox.setFont(QFont('Arial', 15))

        # Button for running algorithm
        self.submitButton = QtWidgets.QPushButton(self)
        self.submitButton.setText("Submit Query")
        self.submitButton.setFont(QFont('Arial', 15))
        self.submitButton.clicked.connect(self.onClick)
        self.submitButton.move(590, 20)
        self.submitButton.resize(200, 100)

    def onClick(self):
        userinput = self.queryTextbox.toPlainText()
        self.queryOutput.setText(userinput)


def window():
    app = QApplication(sys.argv)
    win = MyWindow()

    win.show()
    sys.exit(app.exec_())


def main():
    window()


if __name__ == '__main__':
    main()
