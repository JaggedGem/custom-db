<h3 align="center">Custom DB</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]()
[![GitHub Issues](https://img.shields.io/github/issues/JaggedGem/custom-db.svg)](https://github.com/JaggedGem/custom-db/issues)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/JaggedGem/custom-db.svg)](https://github.com/JaggedGem/custom-db/pulls)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="center"> An attempt at a custom database structure written in TypeScript.
    <br> 
</p>

## Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Deployment](#deployment)
- [Usage](#usage)
- [Built Using](#built_using)
- [TODO](../TODO.md)
- [Contributing](../CONTRIBUTING.md)
- [Authors](#authors)
- [Acknowledgments](#acknowledgement)

## About <a name = "about"></a>

This project is meant to implement a custom database format with a custom language for querying/performing operations on that database.

Features this should include:
- store key value pairs
- persist data to file
- basic indexing
- query language

<sub>P.S. I couldn't think of a name so it's gonna be called CDB (short for Custom DataBase)</sub>

## Getting Started <a name = "getting_started"></a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites

To run this app you'll need to have Git and NodeJS installed

#### For Arch Linux
```bash
sudo pacman -S git nodejs npm
```

#### For Debian based systems
```bash
sudo apt update
sudo apt install git nodejs npm
```

#### For Windows
```bash
winget install Git.Git
winget install OpenJS.NodeJS
```

### Installing

To run the app you first must clone the repository
```bash
git clone https://github.com/JaggedGem/custom-db.git
cd custom-db
```

After that install all Node dependencies with your preffered package manager
```bash
pnpm install # npm/yarn install
```

## Usage <a name="usage"></a>

To run the project simply run
```bash
pnpm run dev # npm/yarn run dev
```

## Authors <a name = "authors"></a>

- [@JaggedGem](https://github.com/JaggedGem) - Idea & Initial work

See also the list of [contributors](https://github.com/JaggedGem/custom-db/contributors) who participated in this project.