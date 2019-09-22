#!/bin/env bash

TOX21_ALL_URL="https://tripod.nih.gov/tox21/challenge/download?id=tox21_10k_data_allsdf"
TOX21_TEST_URL="https://tripod.nih.gov/tox21/challenge/download?id=tox21_10k_challenge_testsdf"
TOX21_SCORE_URL="https://tripod.nih.gov/tox21/challenge/download?id=tox21_10k_challenge_scoresdf"
TOX21_SCORE_CSV_URL="https://tripod.nih.gov/tox21/challenge/download?id=tox21_10k_challenge_score.txt"


if [[ -z ${TOX21_DIR} ]]; then
    echo "Tox21 output dir is not set";
    echo "Set TOX21_DIR evironment variable and rerun script";
    echo "Example: TOX21_DIR=/data bash download_tox21.sh"
    exit
fi

mkdir -p "${TOX21_DIR}"
wget -O "${TOX21_DIR}/tox21_10k_data_all.sdf.zip" "${TOX21_ALL_URL}"
wget -O "${TOX21_DIR}/tox21_10k_data_test.sdf.zip" "${TOX21_TEST_URL}"
wget -O "${TOX21_DIR}/tox21_10k_data_score.sdf.zip" "${TOX21_SCORE_URL}"
wget -O "${TOX21_DIR}/tox21_10k_data_score.txt" "${TOX21_SCORE_CSV_URL}"
