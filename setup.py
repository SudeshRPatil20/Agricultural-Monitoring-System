from setuptools import find_packages,setup
from typing import List

def get_requirements()->List[str]:
    requirements_lst:List[str]=[]
    
    try:
        with open('requirements.txt','r') as file:
            #read lines from file
            lines=file.readlines()
            ## process each line
            for line in lines:
                requirement=line.strip()
                #ignore emty line and -e.
                if requirement and requirement != '-e.':
                    requirements_lst.append(requirement)
    except FileNotFoundError:
        print("requirement.txt file not found")
    
    return requirements_lst


# print(get_requirements())

#check the setup is workning or not by printing requirement_lst

setup(
    name="Satsure_Assignment",
    version="0.0.1",
    author="SudeshPatil",
    author_email="sudeshrpatil201@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements()
)