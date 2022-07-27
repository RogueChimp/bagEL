pip install ./bagel

cd sources
for path in ./*; do
    [ -d "${path}" ] || continue # if not a directory, skip
    dirname="$(basename "${path}")"
    cd $dirname
    [ -f "requirements.txt" ] && pip install -r requirements.txt
    cd ..
done
cd ..