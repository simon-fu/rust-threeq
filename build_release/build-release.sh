
echo "start..." \
&& cargo build --release \
&& rm -rf release.latest/* \
&& mkdir -p release.latest \
&& mkdir -p release.latest/bin \
&& cp -r ./script/* release.latest/ \
&& cp -r ../broker/config release.latest/ \
&& cp ../target/release/rbench release.latest/bin/ \
&& cp ../target/release/rbroker release.latest/bin/ \
&& cp ../target/release/rtools release.latest/bin/ \
&& tar -zcvf release.latest.tar.gz release.latest \
&& echo "done"

