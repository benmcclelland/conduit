// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import "testing"

func TestServerAPIPath(t *testing.T) {
	tests := []struct {
		name    string
		ftaPath string
		ftaRoot string
		apiRoot string
		want    string
		wantErr bool
	}{
		{
			name:    "same mount path",
			ftaPath: "/server/scoutfs/a/b",
			ftaRoot: "/server/scoutfs",
			apiRoot: "/server/scoutfs",
			want:    "/server/scoutfs/a/b",
		},
		{
			name:    "subdirectory NFS export",
			ftaPath: "/client/nfs/scoutfs/a/b",
			ftaRoot: "/client/nfs/scoutfs",
			apiRoot: "/server/scoutfs/dir",
			want:    "/server/scoutfs/dir/a/b",
		},
		{
			name:    "FTA path outside mount",
			ftaPath: "/client/nfs/other/a",
			ftaRoot: "/client/nfs/scoutfs",
			apiRoot: "/server/scoutfs",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := serverAPIPath(test.ftaPath, test.ftaRoot, test.apiRoot)
			if test.wantErr {
				if err == nil {
					t.Fatal("serverAPIPath() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("serverAPIPath() error = %v", err)
			}
			if got != test.want {
				t.Errorf("serverAPIPath() = %q, want %q", got, test.want)
			}
		})
	}
}
