FROM fedora:34

COPY umbra/ /umbra/
COPY create-role.sql /umbra/create-role.sql

ENV LD_LIBRARY_PATH=/umbra/lib/

EXPOSE 5432

ENTRYPOINT ["tail"]
CMD ["-f", "/dev/null"]

