
build:
	@dune build @install
	dd if=/dev/zero of="_build/default/chamelon_fuse/chamelon/src/image.img" bs=64K count=4000
	_build/default/chamelon_fuse/chamelon/src/chamelon.exe format "_build/default/chamelon_fuse/chamelon/src/image.img" 512

install: build
	@dune install

uninstall: build
	@dune uninstall

clean:
	@dune clean
	
chamelon_fuse:
	@dune build chamelon_fuse/hello.exe
	@dune build chamelon_fuse/chamelon_fuse.exe

.PHONY: build install uninstall clean chamelon_fuse

